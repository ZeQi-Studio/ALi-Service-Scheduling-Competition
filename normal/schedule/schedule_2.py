import logging
from base.utils import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    task_list = load_task_list("data/work_order.csv")
    expert_list = load_expert_list("data/process_time_matrix.csv")
    task_expert_dict = load_task_expert_dict("data/process_time_matrix.csv")

    logger.debug(task_list)
    logger.debug(expert_list)
    logger.debug(task_expert_dict)

    # 设置共计三个队列
    #   等待队列（未到达的任务队列）
    not_arrive_list = task_list.copy()
    #   到达但未分配
    arrive_wait_list = []
    #   到达已经分配
    arrive_processing_list = []

    # 循环每一分钟
    timer = 0
    while True:
        timer += 1

        logger.warning(">>> Current time: %s min, in processing: %s, waiting: %s, not arrive: %s", timer,
                       len(arrive_processing_list),
                       len(arrive_wait_list),
                       len(not_arrive_list))
        logger.debug("All expert status: %s", expert_list)

        # 检查哪些任务完成了
        logger.debug(arrive_processing_list)
        remove_list = []

        for task in arrive_processing_list:
            # print(task.task_type)
            # print(task.assigned_expert)

            logger.debug("Task ID: %s, Begin time: %s, Processing time: %s, Timer: %s",
                         task.id,
                         task.begin_time,
                         task.assigned_expert.skill[task.task_type],
                         timer)

            if task.begin_time + task.assigned_expert.skill[task.task_type] == timer:
                # 任务完成
                remove_list.append(task)  # 先放到一个删除列表中，之后再统一删除
                task.assigned_expert.finish(task)
                logger.info("[Finish] Task %s -> Expert %s", task.id, task.assigned_expert.id)
            elif task.begin_time + task.assigned_expert.skill[task.task_type] <= timer:
                # 明明应该早就完成的任务，还在列表中，错误
                raise ValueError
        for task in remove_list:
            arrive_processing_list.remove(task)

        # 结束条件：全部队列空
        if not not_arrive_list and not arrive_wait_list and not arrive_processing_list:
            break

        # 队列调度

        # 找到在此timer对应的时刻，有哪些任务到了
        current_arrived_task_list = []
        remove_list = []
        for task in not_arrive_list:
            if task.arrive_time == timer:
                # 加入到新的临时列表，从未到达的列表中移出
                current_arrived_task_list.append(task)
                remove_list.append(task)
        for task in remove_list:
            not_arrive_list.remove(task)
        current_arrived_task_list = arrive_wait_list + current_arrived_task_list
        arrive_wait_list = []

        # 尝试分配新到达的任务
        for task in current_arrived_task_list:

            # 看合适的专家中是否有空闲的，把他们列出来
            suitable_expert_list = []
            for suitable_expert in task_expert_dict[task.task_type]:
                expert_id = suitable_expert[0]
                # 专家可以再被分配任务
                if expert_list[expert_id - 1].processing_task_num < 3:
                    assert expert_list[expert_id - 1].id == expert_id
                    suitable_expert_list.append(expert_list[expert_id - 1])

            if not suitable_expert_list:  # 没有合适的专家，列表为空
                arrive_wait_list.append(task)
            else:  # 有合适的专家可以用来分配
                arrive_processing_list.append(task)

                # 从中选择一个专家，分配任务
                suitable_expert_list.sort(key=lambda value: value.skill[task.task_type])
                suitable_expert_list[0].assign(task, timer)
                logger.info("[Assign] Task %s Type %s -> Expert %s",
                            task.id,
                            task.task_type,
                            suitable_expert_list[0].id)
