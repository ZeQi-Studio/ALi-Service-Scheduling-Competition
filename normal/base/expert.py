import numpy as np

expert_path = r'../../data/process_time_matrix.csv'


class expert:
    def __init__(self, path):
        self.__expert_path__ = path
        self.expert_processing_task_num = []
        self.__expert_task_dict__ = {}
        self.sorted_expert_task_dict = {}
        self.expert_num = 0
        self.question_num = 0

        self.expert_list = None
        self.__sort_expert_task_dict__()

    def __init_expert__(self):
        with open(self.__expert_path__, encoding='utf-8') as f:
            expert_list = np.loadtxt(f, delimiter=",").astype(int)
            # print((expert_list[1]))
            self.expert_num = len(expert_list) - 1
            self.question_num = len(expert_list[0]) - 1
            self.expert_processing_task_num = [3 for i in range(self.expert_num + 1)]
        self.expert_list = expert_list
        return expert_list

    def __get_expert_task_dict__(self):
        """
        :param expert_list : 初始化获取的专家处理任务列表
        :variable i : 每列的信息，对应任务
        :variable j :每行的信息，对应专家
        :variable temp_dict:能处理i任务的专家j（防止专家编号丢失）
        :return expert_task_dict:返回每个任务所对应的专家及其时间的字典
        """
        expert_list = self.__init_expert__()
        for i in range(1, self.question_num + 1):
            temp_dict = {}
            for j in range(1, self.expert_num + 1):
                if expert_list[j][i] < 999999:
                    temp_dict[j] = expert_list[j][i]
            self.__expert_task_dict__[i] = temp_dict
        # print(self.__expert_task_dict__)

    def __sort_expert_task_dict__(self):
        """"
        :param expert_task_dict:对能处理任务i的专家按照处理时间进行排序
        :param sorted_exoer_task_dict:对每个任务的专家序列按照时间从小到大进行排序，返回(专家序号，处理时间)的序列
        """
        self.__get_expert_task_dict__()
        for i in range(1, self.question_num + 1):
            self.sorted_expert_task_dict[i] = sorted(self.__expert_task_dict__[i].items(), key=lambda value: value[1])
        # print(self.sorted_expert_task_dict)

