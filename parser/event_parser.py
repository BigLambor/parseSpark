"""
事件解析模块 - 核心解析逻辑
"""

import json
from collections import defaultdict
from models.app_metrics import AppMetrics, ExecutorMetrics
from models.stage_metrics import StageMetrics, JobMetrics
from parser.metrics_calculator import MetricsCalculator


class ApplicationState:
    """应用状态管理器"""
    
    def __init__(self, cluster_name, target_date, collect_tasks=True):
        self.cluster_name = cluster_name
        self.target_date = target_date
        self.collect_tasks = collect_tasks
        
        # 应用级别
        self.app_id = None
        self.app_name = None
        self.start_time = None
        self.end_time = None
        self.user = None
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
            user=self.user or 'unknown',
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


class EventLogParser:
    """EventLog解析器"""
    
    @staticmethod
    def parse_file(file_path, cluster_name, target_date, collect_tasks=True):
        """
        解析单个EventLog文件
        :param file_path: 文件路径
        :param cluster_name: 集群名称
        :param target_date: 目标日期
        :param collect_tasks: 是否收集Task级别数据
        :return: 解析结果字典
        """
        from pyspark import SparkContext
        
        # 获取当前SparkContext (在Executor端)
        sc = SparkContext._active_spark_context
        
        # 获取Hadoop FileSystem
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jsc.hadoopConfiguration()
        )
        
        path = sc._jvm.org.apache.hadoop.fs.Path(file_path)
        
        # 修复问题2: 检测并处理压缩格式（.lz4, .snappy等）
        # 使用Hadoop CompressionCodecFactory自动检测压缩格式
        codec_factory = sc._jvm.org.apache.hadoop.io.compress.CompressionCodecFactory(
            sc._jsc.hadoopConfiguration()
        )
        codec = codec_factory.getCodec(path)
        
        # 打开文件流
        raw_input_stream = fs.open(path)
        
        # 如果文件是压缩的，创建解压流；否则直接使用原始流
        if codec is not None:
            input_stream = codec.createInputStream(raw_input_stream)
        else:
            input_stream = raw_input_stream
        
        # 创建应用状态
        app_state = ApplicationState(cluster_name, target_date, collect_tasks)
        
        # 修复P0问题：初始化reader变量，确保finally块可以访问
        reader = None
        
        # 逐行解析
        try:
            # 使用Java BufferedReader读取
            reader = sc._jvm.java.io.BufferedReader(
                sc._jvm.java.io.InputStreamReader(input_stream, "UTF-8")
            )
            
            line = reader.readLine()
            line_count = 0
            
            while line is not None:
                line_count += 1
                
                try:
                    # 解析JSON
                    event = json.loads(str(line))
                    event_type = event.get('Event')
                    
                    # 分发到对应的处理函数
                    EventLogParser._handle_event(event_type, event, app_state)
                
                except json.JSONDecodeError as e:
                    # 修复问题2: 压缩文件解析失败时提供更明确的错误信息
                    # 如果遇到JSONDecodeError且文件是压缩的，可能是压缩格式问题
                    if codec is not None:
                        raise Exception(
                            f"解析压缩文件失败: {file_path}, "
                            f"压缩格式: {codec.getClass().getSimpleName()}, "
                            f"JSON解析错误: {e}"
                        )
                    # 非压缩文件的无效JSON行，跳过
                    pass
                except Exception as e:
                    # 记录错误但继续处理
                    pass
                
                line = reader.readLine()
            
            # 正常流程中关闭reader（会自动关闭底层流）
            reader.close()
            reader = None
        
        except Exception as e:
            # 异常时记录错误，资源关闭交给finally块处理
            raise Exception(f"解析文件失败: {file_path}, 错误: {e}")
        
        finally:
            # 修复P0问题：统一在finally块中关闭资源，避免重复关闭
            # 如果reader未关闭，先关闭reader；否则直接关闭底层流
            try:
                if reader is not None:
                    reader.close()
                elif codec is not None:
                    # 使用了压缩，关闭解压流（会自动关闭底层流）
                    if input_stream is not None:
                        input_stream.close()
                else:
                    # 未使用压缩，关闭原始流
                    if raw_input_stream is not None:
                        raw_input_stream.close()
            except:
                pass
        
        # 返回解析结果
        return {
            'app': app_state.to_app_metrics(),
            'jobs': app_state.to_job_metrics(),
            'stages': app_state.to_stage_metrics(),
            'executors': app_state.to_executor_metrics()
        }
    
    @staticmethod
    def _handle_event(event_type, event, app_state):
        """处理单个事件"""
        
        if event_type == 'SparkListenerApplicationStart':
            app_state.app_id = event.get('App ID')
            app_state.app_name = event.get('App Name')
            app_state.start_time = event.get('Timestamp')
            app_state.user = event.get('User')
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

