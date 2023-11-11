from .ExtractorBase import Base
from ...refactored.executable import Executable
from ...refactored.util.utils import get_escape_string


class GroupByBase(Base):

    def __init__(self, connectionHelper,
                 name,
                 core_relations,
                 join_graph,
                 global_min_instance_dict):
        super().__init__(connectionHelper, name)
        self.app = Executable(connectionHelper)

        self.core_relations = core_relations
        self.global_join_graph = join_graph
        self.global_min_instance_dict = global_min_instance_dict
      

    def extract_params_from_args(self, args):
        return args[0]

