"""
事件解析模块 - 核心解析逻辑
"""

import json
from collections import defaultdict
from models.app_metrics import AppMetrics, ExecutorMetrics
from models.stage_metrics import StageMetrics, JobMetrics
from models.sql_metrics import SQLMetrics, SparkConfigMetrics
from parser.metrics_calculator import MetricsCalculator
from parser.config_filter import normalize_filter, sanitize_items


class ApplicationState:
    """应用状态管理器"""
    
    def __init__(self, cluster_name, target_date, collect_tasks=True, config_filter=None):
        self.cluster_name = cluster_name
        self.target_date = target_date
        self.collect_tasks = collect_tasks
        self.config_filter = config_filter or {}
        self.config_count = 0
        
        # 应用级别
        self.app_id = None
        self.app_name = None
        self.start_time = None
        self.end_time = None
        self.app_user = None
        self.spark_version = None
        self.status = 'RUNNING'
        
        # Job级别
        self.jobs = {}  # job_id -> job_info
        
        # Stage级别
        self.stages = {}  # stage_id -> 最终完成的stage信息（最后一次attempt）
        self.stage_attempts = {}  # (stage_id, attempt_id) -> 当前attempt的stage信息
        self.stage_to_job = {}  # stage_id -> job_id
        
        # Task级别（按stage attempt维度存储，防止重试相互污染）
        self.stage_tasks = defaultdict(list)  # (stage_id, attempt_id) -> [task_info, ...]
        
        # Executor级别
        self.executors = {}  # executor_id -> executor_info
        
        # SQL执行级别
        self.sql_executions = {}  # execution_id -> sql_info
        
        # Spark配置参数
        self.spark_configs = {}  # config_key -> config_value
        self.system_properties = {}  # system_property_key -> property_value
        self.java_properties = {}  # java_property_key -> property_value
    
    def to_app_metrics(self):
        """转换为应用指标"""
        duration_ms = MetricsCalculator.calculate_duration(
            self.start_time, self.end_time
        )
        
        total_cores = sum(e.get('total_cores', 0) for e in self.executors.values())
        total_memory_mb = sum(e.get('max_memory_mb', 0) for e in self.executors.values())
        
        return AppMetrics(
            cluster_name=self.cluster_name,
            app_id=self.app_id or 'unknown',
            app_name=self.app_name or 'unknown',
            start_time=self.start_time or 0,
            end_time=self.end_time,
            duration_ms=duration_ms,
            status=self.status,
            app_user=self.app_user or 'unknown',
            spark_version=self.spark_version or 'unknown',
            executor_count=len(self.executors),
            total_cores=total_cores,
            total_memory_mb=total_memory_mb,
            dt=self.target_date
        )
    
    def to_job_metrics(self):
        """转换为Job指标列表"""
        job_metrics_list = []
        
        for job_id, job_info in self.jobs.items():
            duration_ms = MetricsCalculator.calculate_duration(
                job_info.get('submission_time'),
                job_info.get('completion_time')
            )
            
            job_metrics = JobMetrics(
                cluster_name=self.cluster_name,
                app_id=self.app_id or 'unknown',
                job_id=job_id,
                submission_time=job_info.get('submission_time', 0),
                completion_time=job_info.get('completion_time'),
                duration_ms=duration_ms,
                status=job_info.get('status', 'UNKNOWN'),
                stage_count=len(job_info.get('stage_ids', [])),
                dt=self.target_date
            )
            job_metrics_list.append(job_metrics)
        
        return job_metrics_list
    
    def to_stage_metrics(self):
        """转换为Stage指标列表"""
        stage_metrics_list = []
        
        for stage_id, stage_info in self.stages.items():
            # 计算Task聚合统计
            # 优先使用预计算的统计结果（内存优化），如果不存在则现场计算（兼容未完成Stage）
            agg_stats = stage_info.get('_agg_stats')
            if not agg_stats:
                pending_tasks = []
                for (s_id, _), task_list in self.stage_tasks.items():
                    if s_id == stage_id:
                        pending_tasks.extend(task_list)
                agg_stats = MetricsCalculator.calculate_stage_aggregates(pending_tasks)
            
            duration_ms = MetricsCalculator.calculate_duration(
                stage_info.get('submission_time'),
                stage_info.get('completion_time')
            )
            
            # 获取关联的Job ID
            job_id = self.stage_to_job.get(stage_id, -1)
            
            stage_metrics = StageMetrics(
                cluster_name=self.cluster_name,
                app_id=self.app_id or 'unknown',
                job_id=job_id,
                stage_id=stage_id,
                stage_name=stage_info.get('stage_name', f'Stage {stage_id}'),
                submission_time=stage_info.get('submission_time', 0),
                completion_time=stage_info.get('completion_time'),
                duration_ms=duration_ms,
                status=stage_info.get('status', 'UNKNOWN'),
                input_bytes=stage_info.get('input_bytes', 0),
                input_records=stage_info.get('input_records', 0),
                output_bytes=stage_info.get('output_bytes', 0),
                output_records=stage_info.get('output_records', 0),
                shuffle_read_bytes=stage_info.get('shuffle_read_bytes', 0),
                shuffle_read_records=stage_info.get('shuffle_read_records', 0),
                shuffle_write_bytes=stage_info.get('shuffle_write_bytes', 0),
                shuffle_write_records=stage_info.get('shuffle_write_records', 0),
                num_tasks=stage_info.get('num_tasks', 0),
                num_failed_tasks=stage_info.get('num_failed_tasks', 0),
                task_duration_p50=agg_stats['task_duration_p50'],
                task_duration_p75=agg_stats['task_duration_p75'],
                task_duration_p95=agg_stats['task_duration_p95'],
                task_duration_max=agg_stats['task_duration_max'],
                skew_factor=agg_stats['skew_factor'],
                input_skew_factor=agg_stats['input_skew_factor'],
                peak_memory_max=agg_stats['peak_memory_max'],
                dt=self.target_date
            )
            stage_metrics_list.append(stage_metrics)
        
        return stage_metrics_list
    
    def to_executor_metrics(self):
        """转换为Executor指标列表"""
        executor_metrics_list = []
        
        for executor_id, executor_info in self.executors.items():
            executor_metrics = ExecutorMetrics(
                cluster_name=self.cluster_name,
                app_id=self.app_id or 'unknown',
                executor_id=executor_id,
                host=executor_info.get('host', 'unknown'),
                add_time=executor_info.get('add_time', 0),
                remove_time=executor_info.get('remove_time'),
                total_cores=executor_info.get('total_cores', 0),
                max_memory_mb=executor_info.get('max_memory_mb', 0),
                dt=self.target_date
            )
            executor_metrics_list.append(executor_metrics)
        
        return executor_metrics_list
    
    def to_sql_metrics(self):
        """转换为SQL指标列表"""
        sql_metrics_list = []
        
        for execution_id, sql_info in self.sql_executions.items():
            # 获取时间，确保不为None
            start_time = sql_info.get('start_time') or 0
            end_time = sql_info.get('end_time')
            
            duration_ms = MetricsCalculator.calculate_duration(start_time, end_time)
            
            # 将job_ids列表转换为JSON字符串
            job_ids_str = json.dumps(sql_info.get('job_ids') or [])
            
            sql_metrics = SQLMetrics(
                cluster_name=self.cluster_name,
                app_id=self.app_id or 'unknown',
                execution_id=execution_id,
                sql_text=sql_info.get('sql_text') or '',
                description=sql_info.get('description'),
                physical_plan_description=sql_info.get('physical_plan_description'),
                start_time=start_time,
                end_time=end_time,
                duration_ms=duration_ms,
                job_ids=job_ids_str,
                status=sql_info.get('status') or 'UNKNOWN',
                error_message=sql_info.get('error_message'),
                dt=self.target_date
            )
            sql_metrics_list.append(sql_metrics)
        
        return sql_metrics_list
    
    def to_config_metrics(self):
        """转换为配置指标列表"""
        config_metrics_list = []
        
        # Spark配置参数
        for config_key, config_value in self.spark_configs.items():
            config_metrics = SparkConfigMetrics(
                cluster_name=self.cluster_name,
                app_id=self.app_id or 'unknown',
                config_key=config_key,
                config_value=str(config_value),
                config_category='spark',
                dt=self.target_date
            )
            config_metrics_list.append(config_metrics)
        
        # 系统属性
        for prop_key, prop_value in self.system_properties.items():
            config_metrics = SparkConfigMetrics(
                cluster_name=self.cluster_name,
                app_id=self.app_id or 'unknown',
                config_key=prop_key,
                config_value=str(prop_value),
                config_category='system',
                dt=self.target_date
            )
            config_metrics_list.append(config_metrics)
        
        # Java属性
        for prop_key, prop_value in self.java_properties.items():
            config_metrics = SparkConfigMetrics(
                cluster_name=self.cluster_name,
                app_id=self.app_id or 'unknown',
                config_key=prop_key,
                config_value=str(prop_value),
                config_category='java',
                dt=self.target_date
            )
            config_metrics_list.append(config_metrics)
        
        return config_metrics_list


class EventLogParser:
    """EventLog解析器"""
    
    @staticmethod
    def parse_file(file_path, cluster_name, target_date, collect_tasks=True, config_filter=None):
        """
        解析单个EventLog文件
        :param file_path: 文件路径
        :param cluster_name: 集群名称
        :param target_date: 目标日期
        :param collect_tasks: 是否收集Task级别数据
        :param config_filter: Spark配置过滤策略
        :return: 解析结果字典
        """
        import subprocess
        
        filter_cfg = normalize_filter(config_filter)
        
        # 创建应用状态
        app_state = ApplicationState(cluster_name, target_date, collect_tasks, filter_cfg)
        
        # 初始化 process 变量
        process = None
        
        # 使用 hdfs dfs -text 命令读取文件
        # -text 命令会自动检测并解压压缩格式（如 .lz4, .snappy, .gz 等）
        try:
            process = subprocess.Popen(
                ['hdfs', 'dfs', '-text', file_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,  # 行缓冲
                universal_newlines=False  # 二进制模式，手动解码
            )
            
            line_count = 0
            
            # 逐行读取并解析
            for line_bytes in process.stdout:
                line_count += 1
                
                try:
                    # 解码并解析JSON
                    line = line_bytes.decode('utf-8').strip()
                    if not line:
                        continue
                    
                    event = json.loads(line)
                    event_type = event.get('Event')
                    
                    # 分发到对应的处理函数
                    EventLogParser._handle_event(event_type, event, app_state)
                
                except json.JSONDecodeError:
                    # 无效JSON行，跳过
                    pass
                except UnicodeDecodeError:
                    # 解码错误，跳过该行
                    pass
                except Exception:
                    # 其他错误，继续处理下一行
                    pass
            
            # 等待进程结束并获取退出码
            process.wait()
            
            # 检查是否有错误
            if process.returncode != 0:
                stderr_output = process.stderr.read().decode('utf-8', errors='replace')
                raise Exception(f"hdfs dfs -text 命令失败，退出码: {process.returncode}, 错误: {stderr_output}")
        
        except FileNotFoundError:
            raise Exception(f"找不到 hdfs 命令，请确保 Hadoop 环境已正确配置")
        except Exception as e:
            raise Exception(f"解析文件失败: {file_path}, 错误: {e}")
        finally:
            # 确保子进程资源被清理
            if process is not None:
                try:
                    if process.stdout:
                        process.stdout.close()
                    if process.stderr:
                        process.stderr.close()
                except:
                    pass
        
        # 返回解析结果
        return {
            'app': app_state.to_app_metrics(),
            'jobs': app_state.to_job_metrics(),
            'stages': app_state.to_stage_metrics(),
            'executors': app_state.to_executor_metrics(),
            'sql_executions': app_state.to_sql_metrics(),
            'spark_configs': app_state.to_config_metrics()
        }
    
    @staticmethod
    def _handle_event(event_type, event, app_state):
        """处理单个事件"""
        
        if event_type == 'SparkListenerApplicationStart':
            app_state.app_id = event.get('App ID')
            app_state.app_name = event.get('App Name')
            app_state.start_time = event.get('Timestamp')
            app_state.app_user = event.get('User')
            app_state.spark_version = event.get('Spark Version', 'unknown')
        
        elif event_type == 'SparkListenerApplicationEnd':
            app_state.end_time = event.get('Timestamp')
            if app_state.status != 'FAILED':
                app_state.status = 'FINISHED'
        
        elif event_type == 'SparkListenerJobStart':
            job_id = event.get('Job ID')
            app_state.jobs[job_id] = {
                'submission_time': event.get('Submission Time'),
                'stage_ids': event.get('Stage IDs', []),
                'status': 'RUNNING'
            }
            
            # 记录Stage到Job的映射
            for stage_id in event.get('Stage IDs', []):
                app_state.stage_to_job[stage_id] = job_id
            
            # 建立 SQL 执行与 Job 的关联
            # Spark SQL 通过 Properties 中的 spark.sql.execution.id 关联 Job
            properties = event.get('Properties', {})
            if properties:
                sql_execution_id = properties.get('spark.sql.execution.id')
                if sql_execution_id is not None:
                    try:
                        exec_id = int(sql_execution_id)
                        if exec_id in app_state.sql_executions:
                            if job_id not in app_state.sql_executions[exec_id]['job_ids']:
                                app_state.sql_executions[exec_id]['job_ids'].append(job_id)
                    except (ValueError, TypeError):
                        pass
        
        elif event_type == 'SparkListenerJobEnd':
            job_id = event.get('Job ID')
            if job_id in app_state.jobs:
                app_state.jobs[job_id]['completion_time'] = event.get('Completion Time')
                
                result = event.get('Job Result', {})
                result_type = 'JobSucceeded'
                if isinstance(result, dict):
                    result_type = result.get('Result') or result.get('result') or result_type
                elif result is not None:
                    result_type = str(result)
                
                result_type_lower = result_type.lower()
                job_status = 'FAILED'
                if 'succeed' in result_type_lower or 'success' in result_type_lower:
                    job_status = 'SUCCEEDED'
                
                app_state.jobs[job_id]['status'] = job_status
                if job_status == 'FAILED':
                    app_state.status = 'FAILED'
        
        elif event_type == 'SparkListenerStageSubmitted':
            stage_info = event.get('Stage Info', {})
            stage_id = stage_info.get('Stage ID')
            attempt_id = stage_info.get('Stage Attempt ID', 0)
            stage_key = (stage_id, attempt_id)
            
            app_state.stage_attempts[stage_key] = {
                'stage_id': stage_id,
                'attempt_id': attempt_id,
                'stage_name': stage_info.get('Stage Name', f'Stage {stage_id}'),
                'submission_time': stage_info.get('Submission Time'),
                'num_tasks': stage_info.get('Number of Tasks', 0),
                'status': 'RUNNING',
                'input_bytes': 0,
                'input_records': 0,
                'output_bytes': 0,
                'output_records': 0,
                'shuffle_read_bytes': 0,
                'shuffle_read_records': 0,
                'shuffle_write_bytes': 0,
                'shuffle_write_records': 0,
                'num_failed_tasks': 0
            }
        
        elif event_type == 'SparkListenerStageCompleted':
            stage_info = event.get('Stage Info', {})
            stage_id = stage_info.get('Stage ID')
            attempt_id = stage_info.get('Stage Attempt ID', 0)
            stage_key = (stage_id, attempt_id)
            
            stage_record = app_state.stage_attempts.get(stage_key)
            if not stage_record:
                # 如果缺少提交事件，退化为直接使用完成事件填充
                stage_record = {
                    'stage_id': stage_id,
                    'attempt_id': attempt_id,
                    'stage_name': stage_info.get('Stage Name', f'Stage {stage_id}'),
                    'submission_time': stage_info.get('Submission Time'),
                    'num_tasks': stage_info.get('Number of Tasks', 0),
                    'status': 'RUNNING',
                    'input_bytes': 0,
                    'input_records': 0,
                    'output_bytes': 0,
                    'output_records': 0,
                    'shuffle_read_bytes': 0,
                    'shuffle_read_records': 0,
                    'shuffle_write_bytes': 0,
                    'shuffle_write_records': 0,
                    'num_failed_tasks': 0
                }
            
            stage_record['completion_time'] = stage_info.get('Completion Time')
            # 修复: 正确判断Stage状态 - 有Failure Reason则为FAILED，否则为SUCCEEDED
            stage_record['status'] = 'FAILED' if stage_info.get('Failure Reason') else 'SUCCEEDED'
            stage_record['num_failed_tasks'] = stage_info.get('Number of Failed Tasks', 0)
            # 修复问题1: Stage失败不应该立即标记应用失败，因为Stage可以重试
            # 应用状态应该由Job状态决定，而不是单个Stage的attempt状态
            # 如果Stage重试成功，应用状态应该保持正确
            
            # 提取Task Metrics汇总
            task_metrics = stage_info.get('Task Metrics', {})
            if task_metrics:
                # Input metrics
                input_metrics = task_metrics.get('Input Metrics', {})
                stage_record['input_bytes'] = input_metrics.get('Bytes Read', 0)
                stage_record['input_records'] = input_metrics.get('Records Read', 0)
                
                # Output metrics
                output_metrics = task_metrics.get('Output Metrics', {})
                stage_record['output_bytes'] = output_metrics.get('Bytes Written', 0)
                stage_record['output_records'] = output_metrics.get('Records Written', 0)
                
                # Shuffle read metrics
                shuffle_read = task_metrics.get('Shuffle Read Metrics', {})
                stage_record['shuffle_read_bytes'] = shuffle_read.get('Total Bytes Read', 0)
                stage_record['shuffle_read_records'] = shuffle_read.get('Total Records Read', 0)
                
                # Shuffle write metrics
                shuffle_write = task_metrics.get('Shuffle Write Metrics', {})
                stage_record['shuffle_write_bytes'] = shuffle_write.get('Shuffle Bytes Written', 0)
                stage_record['shuffle_write_records'] = shuffle_write.get('Shuffle Records Written', 0)

            # 内存优化: Stage结束时立即计算聚合指标并释放Task内存
            # 这对于包含大量Task的作业至关重要，防止OOM
            tasks = app_state.stage_tasks.pop(stage_key, [])
            if tasks:
                agg_stats = MetricsCalculator.calculate_stage_aggregates(tasks)
                stage_record['_agg_stats'] = agg_stats
            else:
                stage_record['_agg_stats'] = MetricsCalculator.calculate_stage_aggregates([])
            
            # 将本次attempt的结果作为最终Stage信息
            app_state.stages[stage_id] = stage_record
            app_state.stage_attempts.pop(stage_key, None)
        
        elif event_type == 'SparkListenerTaskEnd':
            if not app_state.collect_tasks:
                return
            task_info = event.get('Task Info', {})
            stage_id = event.get('Stage ID')
            attempt_id = event.get('Stage Attempt ID', 0)
            stage_key = (stage_id, attempt_id)
            
            # 收集Task信息用于聚合统计
            launch_time = task_info.get('Launch Time', 0)
            finish_time = task_info.get('Finish Time', 0)
            duration_ms = max(0, finish_time - launch_time)
            
            task_metrics = event.get('Task Metrics', {})
            input_bytes = 0
            if task_metrics:
                input_metrics = task_metrics.get('Input Metrics', {})
                input_bytes = input_metrics.get('Bytes Read', 0)
            
            task_data = {
                'duration_ms': duration_ms,
                'input_bytes': input_bytes,
                'peak_memory': task_metrics.get('Peak Execution Memory', 0) if task_metrics else 0
            }
            
            app_state.stage_tasks[stage_key].append(task_data)
        
        elif event_type == 'SparkListenerExecutorAdded':
            executor_id = event.get('Executor ID')
            executor_info = event.get('Executor Info', {})
            
            app_state.executors[executor_id] = {
                'host': executor_info.get('Host', 'unknown'),
                'add_time': event.get('Timestamp'),
                'total_cores': executor_info.get('Total Cores', 0),
                'max_memory_mb': executor_info.get('Maximum Memory', 0) // (1024 * 1024)  # bytes to MB
            }
        
        elif event_type == 'SparkListenerExecutorRemoved':
            executor_id = event.get('Executor ID')
            if executor_id in app_state.executors:
                app_state.executors[executor_id]['remove_time'] = event.get('Timestamp')
        
        elif event_type == 'SparkListenerEnvironmentUpdate':
            # 处理环境更新事件，提取Spark配置参数
            environment_info = event.get('Environment Update', {})
            if not environment_info:
                environment_info = event
            
            # 辅助函数：解析属性（兼容列表和字典两种格式）
            def parse_properties(props):
                """解析属性，兼容列表格式和字典格式"""
                result = {}
                if not props:
                    return result
                if isinstance(props, dict):
                    # 字典格式: {"key1": "value1", "key2": "value2"}
                    for key, value in props.items():
                        result[key] = value
                elif isinstance(props, list):
                    # 列表格式: [["key1", "value1"], ["key2", "value2"]]
                    for item in props:
                        if isinstance(item, (list, tuple)) and len(item) >= 2:
                            result[item[0]] = item[1]
                return result
            
            # Spark配置参数
            filter_cfg = app_state.config_filter
            current_count = app_state.config_count

            spark_properties = environment_info.get('Spark Properties', [])
            parsed_spark_props = parse_properties(spark_properties)
            cleaned_spark, current_count = sanitize_items(parsed_spark_props.items(), 'spark', filter_cfg, current_count)
            app_state.spark_configs.update(cleaned_spark)
            
            # 系统属性（排除java.*，由过滤器控制是否采集）
            system_properties = environment_info.get('System Properties', [])
            parsed_system_props = parse_properties(system_properties)
            if filter_cfg.get('collect_system_properties'):
                non_java_props = {k: v for k, v in parsed_system_props.items() if not k.startswith('java.')}
                cleaned_system, current_count = sanitize_items(non_java_props.items(), 'system', filter_cfg, current_count)
                app_state.system_properties.update(cleaned_system)
            
            # Java属性
            if filter_cfg.get('collect_java_properties'):
                java_props = {k: v for k, v in parsed_system_props.items() if k.startswith('java.')}
                cleaned_java, current_count = sanitize_items(java_props.items(), 'java', filter_cfg, current_count)
                app_state.java_properties.update(cleaned_java)

            app_state.config_count = current_count
        
        elif event_type == 'SparkListenerSQLExecutionStart':
            # 处理SQL执行开始事件
            # 兼容不同Spark版本的字段名
            execution_id = (event.get('executionId') or 
                          event.get('Execution ID') or 
                          event.get('execution_id'))
            if execution_id is None:
                return
            
            # 统一转换为整数类型
            try:
                execution_id = int(execution_id)
            except (ValueError, TypeError):
                return  # 无法转换则跳过
            
            # 提取SQL文本（优先顺序：sqlText > description > physicalPlanDescription）
            sql_text = (event.get('sqlText') or 
                        event.get('SQL Text') or 
                        event.get('sql') or
                        None)
            
            description = event.get('description') or ''
            
            # 提取物理执行计划描述（如果存在）
            physical_plan = (event.get('physicalPlanDescription') or 
                            event.get('Physical Plan Description') or
                            event.get('physicalPlan') or
                            '')
            
            # 如果sql_text为None或空字符串，使用description作为fallback
            # 但优先使用非空的sql_text
            final_sql_text = sql_text if (sql_text and sql_text.strip()) else (description if description else '')
            
            # 获取开始时间，确保不为None
            start_time = (event.get('timestamp') or 
                         event.get('time') or 
                         event.get('Timestamp'))
            
            app_state.sql_executions[execution_id] = {
                'execution_id': execution_id,
                'sql_text': final_sql_text,
                'description': description,
                'physical_plan_description': physical_plan,
                'start_time': start_time or 0,
                'end_time': None,
                'job_ids': [],
                'status': 'RUNNING',
                'error_message': None
            }
        
        elif event_type == 'SparkListenerSQLExecutionEnd':
            # 处理SQL执行结束事件
            # 兼容不同Spark版本的字段名
            execution_id = (event.get('executionId') or 
                          event.get('Execution ID') or
                          event.get('execution_id'))
            if execution_id is None:
                return
            
            # 统一转换为整数类型
            try:
                execution_id = int(execution_id)
            except (ValueError, TypeError):
                return  # 无法转换则跳过
            
            # 如果缺少开始事件，创建一个不完整的记录（EventLog可能不完整）
            if execution_id not in app_state.sql_executions:
                # 尝试从结束事件中提取基本信息
                sql_text = (event.get('sqlText') or 
                           event.get('SQL Text') or 
                           event.get('sql') or 
                           event.get('description') or 
                           '')
                app_state.sql_executions[execution_id] = {
                    'execution_id': execution_id,
                    'sql_text': sql_text,
                    'description': event.get('description') or '',
                    'physical_plan_description': '',
                    'start_time': 0,  # 未知开始时间
                    'end_time': None,
                    'job_ids': [],
                    'status': 'UNKNOWN',
                    'error_message': None
                }
            
            sql_info = app_state.sql_executions[execution_id]
            sql_info['end_time'] = (event.get('timestamp') or 
                                   event.get('time') or 
                                   event.get('Timestamp'))
            
            # 判断执行状态
            error = (event.get('error') or 
                    event.get('Error') or
                    event.get('exception') or
                    event.get('Exception'))
            if error:
                sql_info['status'] = 'FAILED'
                # 提取错误信息
                if isinstance(error, dict):
                    error_msg = error.get('message') or error.get('Message') or str(error)
                else:
                    error_msg = str(error)
                sql_info['error_message'] = error_msg[:1000]  # 限制错误信息长度
            else:
                sql_info['status'] = 'SUCCEEDED'
            
            # 提取关联的Job IDs（如果在结束事件中存在）
            # 注意：大部分情况下 job_ids 已在 SparkListenerJobStart 中关联
            job_ids = (event.get('jobIds') or 
                      event.get('Job IDs') or
                      event.get('job_ids') or
                      None)
            if job_ids is not None:
                # 合并已有的job_ids，避免覆盖
                existing_job_ids = set(sql_info.get('job_ids', []))
                if isinstance(job_ids, list):
                    existing_job_ids.update(job_ids)
                elif isinstance(job_ids, (int, str)):
                    existing_job_ids.add(job_ids)
                elif hasattr(job_ids, '__iter__'):
                    existing_job_ids.update(job_ids)
                else:
                    existing_job_ids.add(job_ids)
                sql_info['job_ids'] = list(existing_job_ids)

