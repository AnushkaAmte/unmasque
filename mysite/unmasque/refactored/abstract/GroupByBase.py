from .ExtractorBase import Base
from ...refactored.executable import Executable
from ...refactored.util.utils import get_escape_string


class GroupByBase(Base):

    def __init__(self, connectionHelper,
                 name,
                 core_relations,
                 global_min_instance_dict,global_join_graph):
        super().__init__(connectionHelper, name)
        self.app = Executable(connectionHelper)

        self.core_relations = core_relations
        self.global_min_instance_dict = global_min_instance_dict
        self.global_join_graph = global_join_graph
      

    def extract_params_from_args(self, args):
        return args[0]

