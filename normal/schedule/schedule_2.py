import logging
from base.utils import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

if __name__ == '__main__':
    task_list = load_task_list("data/work_order.csv")
    expert_list = load_expert_list("data/process_time_matrix.csv")

    logger.debug(task_list)
    logger.debug(expert_list)

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

        logger.info("Current time: %s min", timer)

        # 检查哪些任务完成了
        for task in arrive_processing_list:
            if task.start_time + task.assigned_expert.skill[task.task_type] == timer:
                # 任务完成
                arrive_processing_list.remove(task)
            elif task.start_time + task.assigned_expert.skill[task.task_type] <= timer:
                # 明明应该早就完成的任务，还在列表中，错误
                raise ValueError

        # 结束条件：全部队列空
        if not not_arrive_list and not arrive_wait_list and not arrive_processing_list:
            break

        # 队列调度
