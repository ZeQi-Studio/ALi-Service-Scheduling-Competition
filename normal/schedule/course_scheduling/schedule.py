__package__ = "normal.schedule.course_scheduling"

import numpy as np
import logging
from .finetune import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def try_allocate(task, expert):
    time = expert.skill[task.task_type]
    arrive = task.arrive_time

    allocate_success = True

    for i in range(time):
        if expert.task_list_binary[arrive + i] <= 2:
            pass
        else:
            allocate_success = False  # can't allocate

    if allocate_success:  # allocate task to that expert, do assign action
        for i in range(time):
            expert.task_list_binary[arrive + i] += 1
        expert.task_list.append(task)

        expert.work_load += time
        task.assigned_expert = expert
        task.begin_time = arrive

        return True
    else:
        return False


if __name__ == '__main__':
    expert_list = load_expert_list("data/process_time_matrix.csv")
    task_list = load_task_list("data/work_order.csv")

    task_amount = len(task_list)
    allocate_counter = 0

    for expert in expert_list:
        trash_bin = []

        skill_list = list(expert.skill.items())
        skill_list.sort(key=lambda value: value[1])

        for skill_type, time in skill_list:
            for task in task_list:
                if task.task_type == skill_type:
                    if try_allocate(task, expert):
                        trash_bin.append(task)
                        allocate_counter += 1

        for task in trash_bin:
            task_list.remove(task)

    length = 0
    for expert in expert_list:
        logger.debug(expert.task_list)
        length += len(expert.task_list)

    assert allocate_counter == length
    logger.info("Tasks allocated: %s", allocate_counter)
    logger.info("Tasks left: %s", len(task_list))
    assert task_amount == allocate_counter + len(task_list)

    work_load = []
    for expert in expert_list:
        work_load.append(expert.work_load)
    logger.debug(work_load)

    work_load = np.array(work_load)
    work_load = work_load / (60 * 8 * 3)
    logger.info("std: %s", np.std(work_load))
