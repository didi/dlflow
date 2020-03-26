from dlflow.mgr import task, model, config
from dlflow.tasks import UNIVERSAL_TAG, DEFAULT_ROOT
from dlflow.utils.locale import i18n

from absl import logging


_DEFAULT_SPARK_TASKS = {"_Encode", "_Predict", "_Merge"}


class WorkflowDAG(object):

    def __init__(self, steps):
        self._edges = dict()
        self._vertices = dict()
        self._ks = "->"
        self._desc = None
        self._dag = None
        self.root = None

        self.steps = [s.strip() for s in steps.split(",")]
        self._build()

    def __str__(self):
        if self._desc is None:
            self._desc = self._vertices_info(self._dag)
        return self._desc

    @property
    def DAG(self):
        return self._dag

    @property
    def vertices(self):
        return self._vertices

    @property
    def edges(self):
        return self._edges

    def _build(self):
        self.root = _Vertex(task[DEFAULT_ROOT], DEFAULT_ROOT)
        self._vertices[DEFAULT_ROOT] = self.root

        vertices = dict()

        def vtx_proc(name):
            vtx_node = task[name]
            vtx_name = vtx_node.__name__
            bind_tasks = vtx_node.bind_tasks

            if vtx_name not in vertices:
                vertices[vtx_name] = _Vertex(vtx_node, name)

            if not bind_tasks:
                return

            if isinstance(bind_tasks, str):
                bind_tasks = [bind_tasks]

            for _name in bind_tasks:
                vtx_proc(_name)

        for step in set(self.steps):
            vtx_proc(step)

        self._vertices.update(vertices)

        if "MODEL.model_name" in config:
            model_cls = model[config.MODEL.model_name]
            if hasattr(model_cls, "cfg"):
                model_cls.cfg(config.MODEL.model_name)

        if "MODEL.input_name" in config:
            input_cls = model[config.MODEL.input_name]
            if hasattr(input_cls, "cfg"):
                input_cls.cfg(config.MODEL.input_name)

        for _vtx_name, _vtx_node in self._vertices.items():
            if hasattr(_vtx_node.node, "cfg"):
                _vtx_node.node.cfg(_vtx_name)

        edge_list = []
        needs = {}
        product = {}
        for cur_name, cur_vtx in self._vertices.items():
            needs[cur_name] = []
            for p_tag in cur_vtx.node.parent_tag:
                needs[cur_name].append(p_tag)

            for o_tag in cur_vtx.node.output_tag:
                if o_tag not in product:
                    product[o_tag] = []
                product[o_tag].append(cur_name)

        for universal_task in product.get(UNIVERSAL_TAG, []):
            total_tasks = list(self._vertices.keys())
            total_tasks.remove(universal_task)

            for _task in total_tasks:
                edge_list.append((universal_task, _task, UNIVERSAL_TAG))

        for current, need_list in needs.items():
            for _need in need_list:
                for _parent in product.get(_need, []):
                    edge_list.append((_parent, current, _need))

        for head, tail, desc in edge_list:
            e_key = self._ks.join([head, tail])

            if e_key in self.edges:
                self.edges[e_key].add_param(desc)

            else:
                head_node = self._vertices[head]
                tail_node = self._vertices[tail]

                new_edge = _Edge(head_node, tail_node, params=desc)
                self.edges[e_key] = new_edge

        self._dag = self._topology_sort()

        enter_edge = _Edge(None, self.root, None)
        enter_key = self._ks.join(["_None", DEFAULT_ROOT])
        self._edges[enter_key] = enter_edge

    def _topology_sort(self):
        vertices_set = set(self._vertices.values())
        for vtx in vertices_set:
            vtx.tmp_in_degree = vtx.in_degree

        sorted_vertices = []
        while vertices_set:
            _kick_set = set()
            for vtx in vertices_set:
                if vtx.tmp_in_degree == 0:
                    for edge in vtx.out_edges:
                        edge.tail.tmp_in_degree -= 1
                    sorted_vertices.append(vtx)
                    _kick_set.add(vtx)
                    del vtx.tmp_in_degree

            if not _kick_set:
                _safe_set = set()
                for vtx in vertices_set:
                    if vtx.out_degree == 0 or vtx.tmp_in_degree == 0:
                        _safe_set.add(vtx)
                vertices_set -= _safe_set

                err_info = i18n(
                    "Can't build DAG, circular references "
                    "are found, please check it!\n{}") \
                    .format(self._vertices_info(vertices_set))

                logging.error(err_info)
                raise RuntimeError(err_info)

            vertices_set -= _kick_set

        return sorted_vertices

    @staticmethod
    def _vertices_info(vertices_set):
        info = i18n("\nVertices node info:")
        detail = i18n("\nDetail about neighbor:")

        if vertices_set is None:
            info += "\n{:>16}".format("None")
            detail += "\n{:>16}".format("None")
        else:
            for vtx in vertices_set:
                info += "\n{:>16}: {:<16} head: {:<2} tail: {:<2}".format(
                    vtx.name, vtx.node.__name__, vtx.in_degree, vtx.out_degree)

                head_vtx = []
                for in_edge in vtx.in_edges:
                    head_vtx.append(in_edge.head.name)

                tail_vtx = []
                for out_edge in vtx.out_edges:
                    tail_vtx.append(out_edge.tail.name)

                detail += "\n{:>16}: {:<16} head: [{}]  /  tail: [{}]".format(
                    vtx.name,  vtx.node.__name__,
                    ", ".join(head_vtx), ", ".join(tail_vtx))

        return "{}{}".format(info, detail)


class _Edge(object):

    def __init__(self, head_vertex, tail_vertex, params=None):
        self._head = head_vertex
        self._tail = tail_vertex
        self._params = []

        if isinstance(head_vertex, (_Vertex, )):
            self.head.set_out_edge(self)

        if isinstance(tail_vertex, (_Vertex, )):
            self.tail.set_in_edge(self)

        if params is not None:
            self.add_param(params)

    @property
    def head(self):
        return self._head

    @property
    def tail(self):
        return self._tail

    def add_param(self, params):
        self._params.append(params)

    def get_params(self):
        return self._params


class _Vertex(object):

    def __init__(self, node, name):
        self.node = node
        self.name = name
        self._in_edges = []
        self._out_edges = []

    @property
    def in_degree(self):
        return len(self._in_edges)

    @property
    def out_degree(self):
        return len(self._out_edges)

    @property
    def in_edges(self):
        return self._in_edges

    @property
    def out_edges(self):
        return self._out_edges

    def set_in_edge(self, edge):
        self._in_edges.append(edge)

    def set_out_edge(self, edge):
        self._out_edges.append(edge)


class WorkflowRunner(object):

    def __init__(self, param=None):
        self._tasks = []
        self._dag_obj = None
        if param is not None:
            self.bind_workflow(param)

    @property
    def workflow(self):
        return self._dag_obj

    @property
    def DAG(self):
        if not isinstance(self._dag_obj, (WorkflowDAG, )):
            raise AttributeError()
        return self._dag_obj.DAG

    def __enter__(self):
        return self

    def __exit__(self, err_type, err_value, traceback):
        self.close()

    def _initialize(self):
        if self._dag_obj is None:
            raise RuntimeError(
                i18n("WorkflowDAG is not bound, please bind it first!"))

        if config.conf is None:
            raise RuntimeError(
                i18n("Bad configurations, please load it first!"))
        config.initialize()

        for vtx in self._dag_obj.DAG:
            self._tasks.append(vtx.node())

    def bind_workflow(self, param):
        if isinstance(param, (str, )):
            self._dag_obj = WorkflowDAG(param)
        elif isinstance(param, (WorkflowDAG, )):
            self._dag_obj = param
        else:
            raise TypeError(i18n("Parameter 'param' expect type are 'str', "
                                 "'WorkflowDAG' or 'None', but find '{}'!")
                            .format(type(param)))

    def run_task(self):
        self._initialize()

        for _task in self._tasks:
            _task.run()

    def close(self):
        from dlflow.utils.sparkapp import SparkBaseApp, HDFS

        SparkBaseApp().close()
        HDFS().close()
