__package__ = "normal.schedule.course_scheduling"

import numpy as np
import logging
from tqdm import tqdm
from .finetune import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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


def try_insert_with_delay(task: Task, expert: Expert, write_change=False):
    # sort by arrive time
    expert_task_list = sorted(expert.task_list.copy(), key=lambda value: value.arrive_time)
    expert_task_list.append(task)

    def reallocate(task, task_list_binary):
        begin = task.arrive_time
        time = expert.skill[task.task_type]

        i = 0
        while True:  # find empty place to insert task
            # 找到空余空间的开头
            if (task_list_binary[begin + i]) <= 2:
                find_insert_place = True
                # 确认是否有time长的空间
                for ii in range(time):
                    if task_list_binary[begin + i + ii] >= 3:
                        find_insert_place = False
                        break
                # 后面空闲时间足够
                if find_insert_place:
                    begin = begin + i
                    break
            i += 1

        # update task list
        for i in range(begin, begin + time):
            task_list_binary[i] += 1

        if write_change:
            task.begin_time = begin

        return begin - task.arrive_time  # delay

    new_task_list_binary = np.zeros((MAX_TIME,))

    delay = 0
    for task in expert_task_list:
        delay += reallocate(task, new_task_list_binary)

    if write_change:
        expert.task_list = expert_task_list
        expert.task_list_binary = new_task_list_binary
        task.assigned_expert = expert
        expert.work_load += expert.skill[task.task_type]

    return delay - expert.delay, new_task_list_binary


def search_insert_with_delay(task, expert_list):
    result_set = []
    for expert in expert_list:
        if task.task_type not in [t[0] for t in expert.skill.items()]:
            continue
        increased_delay, new_task_list_binary = try_insert_with_delay(task, expert)
        result_set.append((increased_delay, expert, new_task_list_binary))

    # sort by increased delay
    result_set.sort(key=lambda value: value[0])

    # error_check(expert_list)

    try_insert_with_delay(task, result_set[0][1],
                          write_change=True)


def error_check(expert_list):
    # check
    for expert in expert_list:
        task_list_binary = np.zeros(MAX_TIME)
        for task in expert.task_list:
            begin = task.begin_time
            time = expert.skill[task.task_type]

            for i in range(begin, begin + time):
                task_list_binary[i] += 1

        for time_cell in task_list_binary:
            if time_cell > 3:
                for time_cell_error in task_list_binary:
                    print(int(time_cell_error), end=" ")
                raise ValueError

    logger.debug("Error check passed~~")


def gen_schedule_result(expert_list):
    with open("submit.csv", "w") as f:
        for expert in expert_list:
            for task in expert.task_list:
                f.write("{},{},{}\n".format(task.id, expert.id, task.begin_time))
        f.flush()


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

    error_check(expert_list)

    # test
    work_load = []
    for expert in expert_list:
        work_load.append(expert.work_load)
    logger.info("Work load after init allocate: %s", work_load)
    work_load = np.array(work_load)
    work_load = work_load / (60 * 8 * 3)
    logger.info("std: %s", np.std(work_load))

    # with delay
    logger.info("Start allocating with delay...")
    trash_bin = []
    for task in tqdm(task_list):
        logger.debug("Allocate task: %s, task left: %s", task, len(task_list) - len(trash_bin))
        search_insert_with_delay(task, expert_list)
        trash_bin.append(task)

    for task in trash_bin:
        task_list.remove(task)
    assert task_amount == sum([len(x.task_list) for x in expert_list])

    # test
    work_load = []
    for expert in expert_list:
        work_load.append(expert.work_load)
    logger.debug("Final workload: %s", work_load)
    work_load = np.array(work_load)
    work_load = work_load / (60 * 8 * 3)
    logger.info("std: %s", np.std(work_load))

    error_check(expert_list)
    gen_schedule_result(expert_list)
