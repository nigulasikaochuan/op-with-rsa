from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER
from ryu.lib import hub
from ryu.lib.packet import packet, ipv4, ethernet
from ryu.ofproto import ofproto_v1_3
from ryu.base.app_manager import lookup_service_brick
import networkx as nx
from CHANG_LIANG import distance
from common_function import send_flow_mod, _build_packet_out_2
from aware_module import Aware


class NetResource(app_manager.RyuApp):
    '''
        利用aware_module 中的拓扑信息创建图
    '''
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"aware": Aware}

    def __init__(self, *args, **kwargs):
        super(NetResource, self).__init__(*args, **kwargs)
        self.name = "resource"
        self.topo = kwargs['aware']
        # # #self.logger.info(self.topo)
        self.distance_between_nodes = distance
        self.remainSlots = {key: [i for i in range(126)] for key in self.distance_between_nodes}

        self.weight = {key: (self.distance_between_nodes[key] / len(self.remainSlots[key])) for key in
                       self.distance_between_nodes}

        self.monitor = hub.spawn(self._monitor)
        self.pre_graph = nx.DiGraph()
        self.graph = nx.DiGraph()  # 节点用switch的dpid表示

    def _monitor(self):
        i = 0
        flag = False
        self._creat_graph()

        while True:
            if not flag:
                if self.graph:
                    # self.logger.info("创建图完毕")
                    flag = True
            if i == 5:
                self._creat_graph()
                # self.show_topology()
                i = 0
            hub.sleep(1)
            i += 1

    def _creat_graph(self):
        self._calc_weight()
        self._creat_graph_by_weight()

    def _calc_weight(self):

        for key in self.distance_between_nodes:
            try:
                self.weight[key] = self.distance_between_nodes[key] / len(self.remainSlots[key])
            except ZeroDivisionError:
                # 某条链路上的剩余资源为0的时候，给这条链路设置一个足够大的权重
                self.weight[key] = 65535000

    def _creat_graph_by_weight(self):
        '''
        topo的属性
            self.name = "aware"
            self.switches = []  (sw 对象，dp Port)
            self.port_of_switches = {}  # (sw.id)--->port_np
            self.LinkBetweenSwitches = {}  # (src.id,dst.id)----->src_port_no,dst_port_no
            self.monitor = hub.spawn(self._monitor)
            self.HostSwitches = {}  # (sw.dpid,port)---->host_ip,host_mac
        :return:
        '''
        if self.topo.switches:
            for src_sw in self.topo.switches:
                for dst_sw in self.topo.switches:
                    if src_sw.dp.id == dst_sw.dp.id:
                        self.graph.add_edge(src_sw.dp.id, dst_sw.dp.id, weight=0)
                        continue
                    if (src_sw.dp.id, dst_sw.dp.id) in self.topo.LinkBetweenSwitches:
                        weight = self.weight.get((src_sw.dp.id, dst_sw.dp.id), 0) + self.weight.get(
                            (dst_sw.dp.id, src_sw.dp.id), 0)
                        # #self.logger.info(weight)
                        self.graph.add_edge(src_sw.dp.id, dst_sw.dp.id, weight=weight)
        #                 #self.logger.info(self.graph)
        # #self.logger.info(self.topo.LinkBetweenSwitches)
        # #self.logger.info(self.graph)

    def k_shortest_paths(self, src, dst, weight='weight', k=1):
        """
            Great K shortest paths of src to dst.
        """
        generator = nx.shortest_simple_paths(self.graph, source=src,
                                             target=dst, weight=weight)
        shortest_paths = []
        try:
            for path in generator:
                if k <= 0:
                    break
                shortest_paths.append(path)
                k -= 1
            return shortest_paths
        except:
            # self.logger.info("No path between %s and %s" % (src, dst))
            # self.logger.info(self.graph)
            pass
    # @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    # def packet_in_handler(self, ev):
    #     msg = ev.msg
    #     datapath = msg.datapath
    #     pkt = packet.Packet(msg.data)
    #     ip_pkt = pkt.get_protocol(ipv4.ipv4)
    #     if ip_pkt:
    #         #self.logger.info("ipv4 processing")
    #         self.shortest_forward(msg, ip_pkt, pkt)
    #     else:
    #         return
    #
    # def shortest_forward(self, msg, ip_pkt, pkt):
    #     datapaths = self.topo.datapaths
    #     ip_src = ip_pkt.src
    #     ip_dst = ip_pkt.dst
    #     src_id = None
    #     dst_id = None
    #     packet_in_datapath = msg.datapath
    #     packet_in_port = msg.match['in_port']
    #     e_type = pkt.get_protocol(ethernet.ethernet).ethertype
    #     #self.logger.info("发送ip包的是交换机{}：源ip地址是{}，目的ip地址是{}".format(packet_in_datapath.id, ip_src, ip_dst))
    #     hub.sleep(0.001)
    #     for (sw_id, sw_port), (host_ip, host_mac) in self.topo.HostSwitches.items():
    #         if ip_src == host_ip:
    #             src_id = sw_id
    #         if ip_dst == host_ip:
    #             dst_id = sw_id
    #         if src_id is not None and dst_id is not None:
    #             break
    #     if src_id != packet_in_datapath.id:
    #         '''
    #             源ip地址的switch id 和 packe_in 报文的switch id 不一样，直接还给交换机进行重新匹配
    #         '''
    #         # #self.logger.info("src_id is {}".format(src_id))
    #         #self.logger.info("应该还给交换机{}，从这个交换机的port{}输入的".format(packet_in_datapath.id, msg.match['in_port']))
    #         ofproto = packet_in_datapath.ofproto
    #         parser = packet_in_datapath.ofproto_parser
    #         # hub.sleep(0.001)
    #         actions = [parser.OFPActionOutput(port=ofproto.OFPP_TABLE)]
    #         packet_out = self._build_packet_out(packet_in_datapath, actions=actions, data=msg.data,
    #                                             inport=msg.match['in_port'])
    #         packet_in_datapath.send_msg(packet_out)
    #         return
    #
    #     if src_id is not None and dst_id is not None:
    #         paths = self.k_shortest_paths(src_id, dst_id, weight='weight', k=3)
    #         if paths:
    #             flow_information = [e_type, ip_src, ip_dst, packet_in_port]
    #             # #self.logger.info("报文的信息是，以太网类型为{}，源{}目的{}，入端口{}".format(e_type, ip_src, ip_dst, packet_in_port))
    #             self.install_flow(datapaths, paths[0], flow_information, msg.buffer_id, data=msg.data)
    #         else:
    #             #self.logger.info("path 不可知")
    #     else:
    #
    #         #self.logger.info("主机位置暂时不可知")
    #         #self.logger.info("{}".format(self.topo.HostSwitches))

    #
    def install_flow(self, datapaths, path, flow_info, buffer_id, data=None):
        # self.logger.info("install flow to path{}".format(path))
        if path is None or len(path) == 0:
            # self.logger.info("Path error!")
            return
        in_port = flow_info[3]
        first_dp = datapaths[path[0]]
        back_info = (flow_info[0], flow_info[2], flow_info[1])
        if len(path) > 2:
            for i in range(1, len(path) - 1):
                port = self.get_port_pair_from_link(path[i - 1], path[i])
                port_next = self.get_port_pair_from_link(path[i], path[i + 1])
                if port and port_next:
                    src_port, dst_port = port[1], port_next[0]
                    datapath = datapaths[path[i]]
                    self.send_ipv4_flow(datapath, flow_info, src_port, dst_port)
                    self.send_ipv4_flow(datapath, back_info, dst_port, src_port)
                    # #self.logger.info("inter_link flow install to dp{}".format(datapath.id))
        if len(path) > 1:
            # the last flow entry: tor -> host
            port_pair = self.get_port_pair_from_link(path[-2], path[-1])

            # #self.logger.info("{}".format(port_pair))

            if port_pair is None:
                # self.logger.info("Port is not found")
                return
            src_port = port_pair[1]

            dst_port = self.get_port(flow_info[2])
            if dst_port is None:
                # self.logger.info("Last port is not found.")
                return

            last_dp = datapaths[path[-1]]
            # #self.logger.info("  flow install to dp{}".format(last_dp.id))
            self.send_ipv4_flow(last_dp, flow_info, src_port, dst_port)
            self.send_ipv4_flow(last_dp, back_info, dst_port, src_port)

            # the first flow entry
            port_pair = self.get_port_pair_from_link(path[0], path[1])
            # #self.logger.info("{}".format(port_pair))
            if port_pair is None:
                # self.logger.info("Port not found in first hop.")
                return

            out_port = port_pair[0]
            # #self.logger.info("ip报文从第一个交换机的端口{}进入".format(in_port))
            # #self.logger.info("flow install to dp{}".format(first_dp.id))
            self.send_ipv4_flow(first_dp, flow_info, in_port, out_port)
            self.send_ipv4_flow(first_dp, back_info, out_port, in_port)

            self.send_packet_out(first_dp, buffer_id, out_port, data, )

        # src and dst on the same datapath
        else:
            out_port = self.get_port(flow_info[2])
            if out_port is None:
                # #self.logger.info("Out_port is None in same dp")
                return

            # def send_flow_mod(datapath, actions, pri, match, idle_time=0, hard_time=0):
            #     '''
            self.send_ipv4_flow(first_dp, flow_info, in_port, out_port)
            self.send_ipv4_flow(first_dp, back_info, out_port, in_port)
            self.send_packet_out(first_dp, buffer_id, out_port, data)

    def send_packet_out(self, datapath, buffer_id, out_port, data):
        # ofproto = datapath.ofproto
        # #self.logger.info("从交换机{}把ip报文转发到端口{}".format(datapath.id, out_port))
        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(port=out_port)]
        packet_out = self._build_packet_out(datapath, actions=actions, data=data)
        datapath.send_msg(packet_out)

    def send_ipv4_flow(self, datapath, flow_info, src_port, dst_port):
        """
            Build flow entry, and send it to datapath.
        """
        parser = datapath.ofproto_parser
        actions = []
        actions.append(parser.OFPActionOutput(dst_port))

        match = parser.OFPMatch(
            in_port=src_port, eth_type=flow_info[0],
            ipv4_src=flow_info[1], ipv4_dst=flow_info[2])

        send_flow_mod(datapath, actions, 1, match,
                      idle_time=40, hard_time=60)

    # def assigment_slot(self, src_id, dst_id):
    #     pass
    #

    def get_port(self, host_ip_):
        # # #self.logger.info("{}".format(self.topo.HostSwitches))
        for (sw_id, sw_port), (host_ip, host_mac) in self.topo.HostSwitches.items():
            if host_ip_ == host_ip:
                return sw_port
        else:
            return None

    def get_port_pair_from_link(self, src_path, dst_path):
        # #self.logger.info("寻找交换机{}和交换机{}之间的端口".format(src_path,dst_path))
        # # #self.logger.info("{}".format(self.topo.LinkBetweenSwitches))
        return self.topo.LinkBetweenSwitches.get((src_path, dst_path), None)

    def _build_packet_out(self, datapath, actions, data, inport=None):
        '''

              :param datapath:这个packout消息要发送到的交换机
              :param actions: 动作必须是可迭代的
              :param data: packetout消息要封装的数据
              :return: 无

              构造packet_out报文
        '''
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        if inport is None:
            packet_out_msg = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER,
                                                 in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=data)
        else:
            packet_out_msg = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER,
                                                 in_port=inport, actions=actions, data=data)
        return packet_out_msg

    # from ryu.ofproto.ofproto_v1_3_parser import OFPPacketOut
    def show_topology(self):
        import copy

        switch_num = len(list(self.graph.nodes()))
        if self.pre_graph != self.graph:
            # #self.logger.info("{}".format(self.graph))
            print("---------------------Topo Link---------------------")
            # print('%10s' % ("switch"), end=' ')
            # for i in self.graph.nodes():
            #     print('%10d' % i, end=' ')
            # print("")
            for i in self.graph.nodes():
                # print('%10d' % i, end=' ')
                for i_j, j in self.graph[i].items():
                    print('{}---->{} weight{} '.format(i, i_j, j['weight']), end=' ')
                print("")
            self.pre_graph = copy.deepcopy(self.graph)

        # if self.pre_link_to_port != self.link_to_port :
        #     print("---------------------Link Port---------------------")
        #     print('%10s' % ("switch"), end=' ')
        #     for i in self.graph.nodes():
        #         print('%10d' % i, end=' ')
        #     print("")
        #     for i in self.graph.nodes():
        #         print('%10d' % i, end=' ')
        #         for j in self.graph.nodes():
        #             if (i, j) in self.link_to_port.keys():
        #                 print('%10s' % str(self.link_to_port[(i, j)]), end=' ')
        #             else:
        #                 print('%10s' % "No-link", end=' ')
        #         print("")
        #     self.pre_link_to_port = copy.deepcopy(self.link_to_port)
        #
        # if self.pre_access_table != self.access_table and setting.TOSHOW:
        #     print("----------------Access Host-------------------")
        #     print('%10s' % ("switch"), '%12s' % "Host")
        #     if not self.access_table.keys():
        #         print("    NO found host")
        #     else:
        #         for tup in self.access_table:
        #             print('%10d:    ' % tup[0], self.access_table[tup])
        #     self.pre_access_table = copy.deepcopy(self.access_table)
