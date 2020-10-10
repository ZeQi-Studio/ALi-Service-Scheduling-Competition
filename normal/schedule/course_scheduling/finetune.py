__all__ = ["Expert", "Task",
           "load_expert_list", "load_task_list",
           # const
           "MAX_TIME"]

import numpy as np

from base.expert import expert as expert_data
from base.utils import Expert as __Expert__, Task as __Task__, load_task_list

MAX_TIME = 5000


class Expert(__Expert__):
    def __init__(self,
                 expert_id,
                 skill_dict):
        super(Expert, self).__init__(expert_id, skill_dict)
        self.task_list = []
        self.task_list_binary = [0 for _ in range(MAX_TIME)]

    def __str__(self):
        return "<id:{},skill:{},processing_task_num:{},word_load:{}>".format(self.id,
                                                                             self.skill,
                                                                             self.processing_task_num,
                                                                             self.work_load)


class Task(__Task__):
    def __str__(self):
        return "<id:{},type:{},start:{},limit:{}>".format(self.id,
                                                          self.task_type,
                                                          self.arrive_time,
                                                          self.time_limit)


def load_expert_list(processing_matrix_file):
    data = expert_data(processing_matrix_file)
    raw_matrix = data.expert_list

    expert_list = []
    for i in range(1, data.expert_num + 1):
        skill_dict = {}
        for j in range(1, data.question_num + 1):
            if raw_matrix[i][j] < 999999:
                skill_dict[j] = raw_matrix[i][j]
        expert_list.append(Expert(expert_id=i,
                                  skill_dict=skill_dict))

    return expert_list
