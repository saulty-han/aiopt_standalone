## 讨论结论
RO/RW AI 优化支持：扩展 tasks 表，支持指定数据源对应的 UUID，workload 预处理模块，支持从指定节点拉取日志，优化规则统一写入 RW，由内核同步机制应用至 RO

任务管理模块移交管控开发、维护

优化环境申请在任务执行前完成，对于 slave，如果超时则训练过程中申请新的 clone 环境

任务分为只读任务、读写任务两类，读写任务要求在任务执行前分配好 clone 环境

性能监控两套，外部展示按照固定时间点进行统计，内部按照实际计划变更来统计


## 补充内容
这四个点会影响用户接口，RO/RW AI 优化支持、任务管理移交管控、优化环境动态切换、任务区分只读/读写。
1. 在进入执行器之前就要申请好优化环境，根据任务类型确定是克隆环境还是Slave或RO，也就意味着需要能够标识环境类型。
2. 执行器只负责单次执行，并在返回值中指示是否执行成功。
3. 执行过程中如果超时动态切换优化环境：对于读写任务，执行前就拿到了克隆环境，无需切换；对于读写任务，如果执行前拿的是内部 RO 环境，也无需切换，因为内部 RO 不影响用户操作；如果之前环境是 slave，超时的情况下申请克隆环境继续后续训练；可以利用 max execution time 来预测下一次验证操作是否会导致超时，但是对于默认计划验证目前没有设定超时，这个需要考虑怎么优化？
4. 数据源这个需要扩展 task 表，具体字段和实现方式有待进一步讨论。

## gaps review 追加内容
- RO/RW AI 优化支持：在 tasks 表添加 node_uuid 字段，用于指定数据源对应的 UUID
- Aduit 添加接口，实现留空即可。

## CDB/NCDB 兼容
- 假设都具备 PlanID 这个 feature
- NCDB 支持 hints 抽取、SPM，CDB 不支持这两个特性，需要对流程进行裁剪以保持兼容
- 现在的负载有效性验证也在启用 hints 抽取和 PlanID 提取两项功能的前提下进行，CDB 中需要对流程进行裁剪以保持兼容
- CDB 和 NCDB 兼容通过检测源数据库中的版本来实现，配置文件中指定两个版本列表

## 其它要点
- 开始训练前在 training server（优化环境）上需要关掉 spm 或者 outline 功能
    - outline: statement_outline_enable_apply
    - spm: txsql_spm_use_plan_baseline

## 关于实现的评论
- 候选计划收集
  - 计划捕获是不是应该统一收敛在 _collect_all_candidates 中，并且根据实际的 feature 和 outline 模式来决定捕获哪些？
  - 只要实例支持 hints 捕获就进行默认计划捕获，并且在计划枚举阶段使用捕获到的 hints 而不是原始 hints
  - 如果实例不支持 hints，那么就无法捕获默认计划，也不会有 SPM 中的计划，计划枚举的计划也只能用原始 hints，只需要获取 PlanID 来标识计划，计划的复现使用原始 hints
  - SPM 中的计划是否获取，取决于 outline 模式

## 关于实现的评论 2
- 不再定义 FeatureSet 枚举，而是通过动态 feature 检测实现（通过检测实例是否存在系统变量），不依赖配置文件和版本号
- 对于不支持 hints 抽取的实例，需要裁剪 SQL 正确性验证模块，也需要裁剪候选计划枚举模块
  - SQL 正确性验证模块完全版本会启用 hints 抽取和 PlanID 两个 feature，然后进行 explain，但是如果不存在 hints 抽取 feature，这个可以不开
  - 候选计划枚举模块也是，如果不存在 hints 抽取 feature，可以在没有这功能的情况下枚举计划，只是 hints_text 就得用枚举结果而非抽取结果
- 在实际调用执行之前，由调度器进行优化环境申请，优化环境直接由调度器传递给执行器
- 执行器类型感知，这个直接就是任务定义的一部分吧，看下咋实现
- 兼容性支持按照第一条说的，动态检测 feature，而不是限定版本号
- Environment Pre-allocation (环境预申请) 是必须要做的，由调度器直接申请好，传递给执行器
- 另外也要实现超时动态切换逻辑，具体参照之前文档

## 关于超时时间
- 应该在配置中添加几个超时时间
  - 对这个实例整个优化流程的超时时间：可设为一个小时，超时则到此为止不再优化，应用已经生成的规则即可
  - slave 环境使用超时时间：设定为 20 分钟，超过这个时间则申请克隆环境继续后续训练
  - 单条 SQL 的超时时间，这是为了避免 SQL 样本的默认计划执行时间过长，导致严重超时
    - 候选计划验证会设置 max_execution_time，就以原先的逻辑为准，不必受到单条 SQL 超时时间的限制（其实是有隐式的关联的，因为 max execution time 会受到单条 SQL 超时时间的限制）
  - 训练过程中，可以计算出来 slave 剩余可用时间，实例训练可用时间，再和预测的单条 SQL 执行时间、设计的 max execution time 进行比较，就能预估一次评估是否会导致超时。如果超时就在执行前预先进行环境切换或者终止优化等操作。
  
## 关于优化规则决策
根据设计文档的论述，默认计划的验证记录需要记下来吗？如果有两个 SQL 样本发现计划 A 是更优计划，而这个计划 A 是第三个 SQL 样本的默认计划，是否应该认为这个机会可以被选出来固定？

## 新需求：AI 优化器训练之后，对于 SPM 修改 origin
AI 优化器处理过的 SPM 规则，无轮船 reject，还是 accept，还是新添加，都把 origin 设置或者修改为 3。

## 优化规则决策讨论补充
真正意图是：
优化计划候选集合选择标准：比默认计划更优或者就是默认计划，这里把默认计划加进去是为了允许一个计划在部分 SQL 样本上存在优化效果且不劣化任何一个样本

最终能够确定一个优选计划集合，但是如果说优选计划集合中的计划，对任何一个样本都不存在相比默认计划更好的效果呢，它也不能作为决策，这没有意义。
这样决策也能避免把公共默认计划给选出来，因为这种场景应该 Default，而不是固定计划。

这样一来就能确定一个有优化效果且不会让任何样本退化的集合。
进一步，确认是否其中存在某一个计划，是所有 SQL 样本的最优计划（假如说一个样本的最优计划不在这个候选集合里面呢？之前由于降低其他 SQL 样本的性能而被排除了呢），如果是则选它，否则就都选出来。

对于 Statement Outline，除非确定唯一最好的计划，否则就 Default。

## validation_logs 表内容？
使用 `select * from validation_logs where plan_id = default_plan_id` 查出来不少内容，这个符合预期吗？
会是代码 BUG 吗，review 排查一下看什么原因？是否符合预期？

## 预热增加配置项
在配置文件中配置预热次数。

## todo
- 确认基于 feature set 检测的兼容机制是否真的有效，需要使用 CDB 20250430 环境进行真实测试
- 环境预申请和动态切换逻辑需要 review，确认是否真正可以工作
- 动态切换逻辑需要适配这个文档前面论述的超时时间部分
- feature 检测中 Outline 的参数不对，应该是：txsql_ai_rules_enabled

## todo 20260123
- 执行器根据信号决定是否终止优化，分两种模式：
  - 一种是在完成当前 SQL 模板的优化之后，终止优化流程并应用当前优化结果
  - 另一种是中断当前 SQL 模板的优化，终止优化流程并应用当前优化结果
- 实际测试优化环境切换的正确性：可以通过修改配置文件中的超时时间来构造测试场景

## CDB/NCDB 训练逻辑
Instance info 表的 inst_id 总是集群标识，无论是 CDB (master, slave, cdb_ro)，还是 NCDB (rw, ncdb_ro)。
Instance info 表的node_uuid 总是标识一个开启 AI 优化功能的节点，无论是 master/cdb_ro/rw/ncdb_ro。
实际上对于 NCDB，Instance info 表的node_uuid 存的就是管控端的 node_uuid，对于 CDB 存的是单个节点的 inst_id？

读取 workload 的时候，
- 对于 NCDB 来说，使用 instance info 的 node_uuid 字段来直接匹配 ck 中的 node_uuid 列
- 对于 CDB 来说，使用 instance info 的 node_uuid 字段来匹配 ck 中的 inst_id 列

优化规则总是为整个集群维护一套，也就是针对 Instance info 表的 inst_id 来维护

写入优化规则时，
- 对于 NCDB 要获取 rw 节点，规则写入 rw 节点
- 对于 CDB 要获取 master 节点，规则写入 master 节点

性能监控模块，也是针对单个节点，也就是 Instance info 中的 node_uuid 来进行。

## CDB/NCDB workload 加载适配
对于 CDB 来说，需要通过 ck 中的 `(inst_id, inst_type)` 来拉取 workload，其中 inst_type 是 master 或者 ro，其中的 inst_id 对应与 Instance info 表中的 node_uuid。
对于 NCDB 来说，需要通过 node_uuid 来拉取 workload，node_uuid 对应与 Instance info 表中的 node_uuid，ck 中的列名也是 node_uuid。

AWR CK 表定义：
```sql
CREATE TABLE cdblog.awr_sql_agg_by_digest_plan_id__101
(
    `timestamp` DateTime,
    `inst_id` String,
    `inst_type` String,
    `node_uuid` String,
    `ip_port` String,
    `db` String,
    `sql_hash` String,
    `plan_id` String,
    `sql_text_example` String,
    `count_star` UInt64,
    `cpu_time_avg` Float64,
    `cpu_time_min` Float64,
    `cpu_time_max` Float64,
    `io_time_avg` Float64,
    `io_time_min` Float64,
    `io_time_max` Float64,
    `lock_time_avg` Float64,
    `lock_time_min` Float64,
    `lock_time_max` Float64,
    `lock_time_mdl_avg` Float64,
    `lock_time_mdl_min` Float64,
    `lock_time_mdl_max` Float64,
    `lock_time_table_avg` Float64,
    `lock_time_table_min` Float64,
    `lock_time_table_max` Float64,
    `lock_time_record_avg` Float64,
    `lock_time_record_min` Float64,
    `lock_time_record_max` Float64,
    `elapsed_time_avg` Float64,
    `elapsed_time_min` Float64,
    `elapsed_time_max` Float64,
    `memory_used_avg` Float64,
    `memory_used_min` Float64,
    `memory_used_max` Float64,
    `rows_scanned_avg` Float64,
    `rows_scanned_min` Float64,
    `rows_scanned_max` Float64,
    `redo_size_avg` Float64,
    `redo_size_min` Float64,
    `redo_size_max` Float64,
    `undo_size_avg` Float64,
    `undo_size_min` Float64,
    `undo_size_max` Float64,
    `binlog_size_avg` Float64,
    `binlog_size_min` Float64,
    `binlog_size_max` Float64,
    `logical_read_avg` Float64,
    `logical_read_min` Float64,
    `logical_read_max` Float64,
    `logical_write_avg` Float64,
    `logical_write_min` Float64,
    `logical_write_max` Float64,
    `bp_hit_rate_avg` Float64,
    `bp_hit_rate_min` Float64,
    `bp_hit_rate_max` Float64,
    `rows_affected_avg` Float64,
    `rows_affected_min` Float64,
    `rows_affected_max` Float64,
    `rows_sent_avg` Float64,
    `rows_sent_min` Float64,
    `rows_sent_max` Float64,
    `tmp_disk_tables_avg` Float64,
    `tmp_disk_tables_min` Float64,
    `tmp_disk_tables_max` Float64,
    `tmp_mem_tables_avg` Float64,
    `tmp_mem_tables_min` Float64,
    `tmp_mem_tables_max` Float64,
    `full_join_count_avg` Float64,
    `full_join_count_min` Float64,
    `full_join_count_max` Float64,
    `full_range_join_count_avg` Float64,
    `full_range_join_count_min` Float64,
    `full_range_join_count_max` Float64,
    `select_range_avg` Float64,
    `select_range_min` Float64,
    `select_range_max` Float64,
    `select_range_check_avg` Float64,
    `select_range_check_min` Float64,
    `select_range_check_max` Float64,
    `select_scan_avg` Float64,
    `select_scan_min` Float64,
    `select_scan_max` Float64,
    `sort_merge_passes_avg` Float64,
    `sort_merge_passes_min` Float64,
    `sort_merge_passes_max` Float64,
    `sort_range_avg` Float64,
    `sort_range_min` Float64,
    `sort_range_max` Float64,
    `sort_rows_avg` Float64,
    `sort_rows_min` Float64,
    `sort_rows_max` Float64,
    `sort_scan_avg` Float64,
    `sort_scan_min` Float64,
    `sort_scan_max` Float64,
    `no_index_used_count_avg` Float64,
    `no_index_used_count_min` Float64,
    `no_index_used_count_max` Float64,
    `no_good_index_used_count_avg` Float64,
    `no_good_index_used_count_min` Float64,
    `no_good_index_used_count_max` Float64,
    `thd_wait_sleep_avg` Float64,
    `thd_wait_sleep_min` Float64,
    `thd_wait_sleep_max` Float64,
    `thd_wait_disk_io_avg` Float64,
    `thd_wait_disk_io_min` Float64,
    `thd_wait_disk_io_max` Float64,
    `thd_wait_row_lock_avg` Float64,
    `thd_wait_row_lock_min` Float64,
    `thd_wait_row_lock_max` Float64,
    `thd_wait_global_lock_avg` Float64,
    `thd_wait_global_lock_min` Float64,
    `thd_wait_global_lock_max` Float64,
    `thd_wait_meta_data_lock_avg` Float64,
    `thd_wait_meta_data_lock_min` Float64,
    `thd_wait_meta_data_lock_max` Float64,
    `thd_wait_table_lock_avg` Float64,
    `thd_wait_table_lock_min` Float64,
    `thd_wait_table_lock_max` Float64,
    `thd_wait_user_lock_avg` Float64,
    `thd_wait_user_lock_min` Float64,
    `thd_wait_user_lock_max` Float64,
    `thd_wait_binlog_avg` Float64,
    `thd_wait_binlog_min` Float64,
    `thd_wait_binlog_max` Float64,
    `thd_wait_group_commit_avg` Float64,
    `thd_wait_group_commit_min` Float64,
    `thd_wait_group_commit_max` Float64,
    `thd_wait_sync_avg` Float64,
    `thd_wait_sync_min` Float64,
    `thd_wait_sync_max` Float64,
    `thd_wait_gcr_avg` Float64,
    `thd_wait_gcr_min` Float64,
    `thd_wait_gcr_max` Float64,
    `thd_wait_last_avg` Float64,
    `thd_wait_last_min` Float64,
    `thd_wait_last_max` Float64,
    `query_cost_avg` Float64,
    `query_cost_min` Float64,
    `query_cost_max` Float64,
    `last_query_start_time` UInt64,
    `store_timestamp` DateTime DEFAULT now(),
    `sql_text_min` String,
    `sql_text_max` String
)
ENGINE = Distributed('default_cluster', 'cdblog', 'awr_sql_agg_by_digest_plan_id__101_physical', rand())
```

SLOW LOG CK 表定义：
```sql
CREATE TABLE ncdblog.slowlog
(
    `timestamp` DateTime,
    `event_path` String,
    `event_host` String,
    `event_module` String,
    `event_dataset` String,
    `event_offset` Int64,
    `event_delay` Int64,
    `event_day` String,
    `event_size` Int64,
    `instid` String,
    `node_uuid` String,
    `clusterid` Int64,
    `insttype` String,
    `ip` String,
    `port` UInt32,
    `log_timestamp` DateTime,
    `md5` String,
    `visible` Bool,
    `need_alarm` Bool,
    `alarm_words` String,
    `content` String,
    `user_host` String,
    `user_name` String,
    `start_time` Int64,
    `query_time` Float64,
    `lock_time` Float64,
    `rows_sent` Int64,
    `rows_examined` Int64,
    `thread_id` Int64,
    `insert_id` Int64,
    `last_insert_id` Int64,
    `server_id` Int64,
    `sql_type` String,
    `database` String,
    `sql_raw_text` String,
    `store_timestamp` DateTime DEFAULT now(),
    `sync_read_count_local` Int64,
    `sync_read_bytes_local` Int64,
    `sync_read_time_local` Int64,
    `sync_write_count_local` Int64,
    `sync_write_bytes_local` Int64,
    `sync_write_time_local` Int64,
    `async_read_count_local` Int64,
    `async_read_bytes_local` Int64,
    `async_write_count_local` Int64,
    `async_write_bytes_local` Int64,
    `sync_read_count_remote` Int64,
    `sync_read_bytes_remote` Int64,
    `sync_read_time_remote` Int64,
    `sync_write_count_remote` Int64,
    `sync_write_bytes_remote` Int64,
    `sync_write_time_remote` Int64,
    `async_read_count_remote` Int64,
    `async_read_bytes_remote` Int64,
    `async_write_count_remote` Int64,
    `async_write_bytes_remote` Int64,
    `trx_commit_delay` Int64
)
ENGINE = Distributed('default_cluster', 'ncdblog', 'slowlog_physical', rand())
```

## TODO! IMPORTANT!
- 防止主从切换时，AI 优化器写入错误的节点：最终写规则之前，确保调用管控接口获取最新 master/rw 节点
- 凡是 AI 优化器修改过的 SPM 规则，都要修改 SOURCE
  - 完全可以这样，只要这个 SQL 模板 SPM 修改过规则，就把它所有的规则设置为 MANUAL-LOAD
  - 问题是：SPM 似乎没有提供修改 source 的接口？暂不处理
- feedback timeout 需要给一个合理的值

## 解耦 AI 优化器与管控
AI 优化器只提供执行器接口，不负责任务提交、任务调度、任务状态管理等。

AI 优化器独立部署训练程序输入参数：
1. 优化环境信息：env_config, env_type
2. 完整的 instance_info 对象（也就是 upgrade_info）
3. Task 相关信息：task_id, task_type=READ_ONLY/READ_WRITE, workload_set
4. master/rw node：实例连接信息
5. operator：操作人

另外，管控也需要提供接口来查询最新的 master/rw 节点信息。

## TODO
- 验证 RESET 操作时的性能监控结果，应该没有验证记录？怎么处理？怎么返回给前端？
- 其实 RESET 一般也会有验证记录，除非枚举的计划全部超时？目前的 RESET 结果可以正常拿到一份验证记录，也会有计划。或许应该验证一下全部超时场景的 RESET 操作？
