# 拓扑发现用的application
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, set_ev_cls, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.lib import hub
from ryu.lib.packet import packet, arp
from ryu.topology import api
from ryu.topology import event
from ryu.ofproto import ofproto_v1_3

from common_function import add_miss_flow
from common_function import _build_packet_out

EventToListened = [event.EventSwitchEnter, event.EventSwitchLeave, event.EventSwitchReconnected,
                   event.EventLinkAdd,
                   event.EventLinkDelete,
                   event.EventPortAdd, event.EventPortDelete, event.EventPortModify, event.EventHostAdd,
                   event.EventHostDelete]


class Aware(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Aware, self).__init__(*args, **kwargs)

        # self.logger.info(self.sw)
        self.datapaths = {}  # dpid---->datapath
        self.name = "aware"
        self.switches = []
        self.port_of_switches = {}  # (sw.id)--->set(port_np)
        self.port_of_switches_remained = {}  # {dpid}--->set(port_no)
        self.LinkBetweenSwitches = {}  # (src.id,dst.id)----->src_port_no,dst_port_no
        self.monitor = hub.spawn(self._monitor)
        self.HostSwitches = {}  # (sw.dpid,port)---->host_ip,host_mac

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.info("switch {} is connected".format(datapath.id))
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.info("switch {} is disconnected".format(datapath.id))
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        add_miss_flow(datapath)

    # @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    # def switch_features_handler(self, ev):
    #
    #     msg = ev.msg
    #     datapath = msg.datapath
    #     self.logger.info("switch {} is connected".format(datapath.id))
    #     self.datapaths[datapath.id] = datapath
    #     add_miss_flow(datapath)

    def _monitor(self):
        i = 0

        while True:

            if i == 5:
                self.topo(None)
                i = 0
            hub.sleep(1)
            i += 1

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_hanler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        parser = datapath.ofproto_parser
        pkt = packet.Packet(msg.data)
        arp_packet = pkt.get_protocol(arp.arp)

        if arp_packet:

            dst_ip = arp_packet.dst_ip
            dst_mac = arp_packet.dst_mac
            # self.logger.info("交换机{}发出arp，目的ip{},目的mac{}".format(datapath.id, dst_ip, dst_mac))
            # self.logger.info("已知的主机列表为{}".format(self.HostSwitches))
            for (sw, sw_port), (host_ip, host_mac) in self.HostSwitches.items():
                # self.logger.info("for ")
                if host_ip == dst_ip:
                    self.arp_forward(sw, sw_port, msg)
                    break
            # 正常循环结束了，都没进行arp转发，说明目标主机不可知，将arp包发送到交换机的所有剩余端口
            else:

                self.flood_arp(msg)
        # if self.HostSwitches:
        #     self.logger.info("{}".format(self.HostSwitches))

    @set_ev_cls(EventToListened)
    def topo(self, ev):
        self.get_switch()
        self.get_port_of_switches()  # dpid---->set
        self.get_link_between_switches()
        self.get_port_of_switches_remained()
        self.get_hosts()
        # self.logger.info(self.switches)
        # self.logger.info("{}".format(self.LinkBetweenSwitches))
        # # self.logger.info(self.HostSwitches)
        # self.logger.info("{}".format(self.port_of_switches_remained))

    def get_switch(self):
        '''

        :return:switches:dp,port  port:dpid,port_no
        '''
        self.switches = api.get_all_switch(self)

    def get_port_of_switches(self):
        for sw in self.switches:
            datapath = sw.dp
            id = datapath.id
            self.port_of_switches.setdefault(id, set())

            for port in sw.ports:
                self.port_of_switches[id].add(port.port_no)

    def get_link_between_switches(self):
        AllLink = api.get_all_link(self)
        for Link in AllLink:
            src_port = Link.src
            dst_port = Link.dst
            src_sw_dpid = src_port.dpid
            dst_sw_dpid = dst_port.dpid
            self.LinkBetweenSwitches[(src_sw_dpid, dst_sw_dpid)] = (src_port.port_no, dst_port.port_no)

    def get_hosts(self):
        hosts = api.get_host(self)

        for host in hosts:
            # self.logger.info("get hosts中，主机的ipv4{}".format(host.ipv4))
            self.HostSwitches[(host.port.dpid, host.port.port_no)] = (host.ipv4[0], host.mac)
        # self.logger.info(self.HostSwitches)

    def get_port_of_switches_remained(self):
        for sw in self.switches:
            dpid = sw.dp.id
            self.port_of_switches_remained.setdefault(dpid, self.port_of_switches[dpid])
            for key in self.LinkBetweenSwitches:
                if key[0] == dpid:
                    # self.logger.info("交换机{}所有的端口是{}".format(dpid,self.port_of_switches[dpid]))
                    self.port_of_switches_remained[dpid] = self.port_of_switches_remained[dpid] - {
                        self.LinkBetweenSwitches[key][0]}
                    # self.logger.info("交换机{}去掉的端口是{}".format(dpid,{self.LinkBetweenSwitches[key][0]}))
                    # self.logger.info("交换机{}剩余{}".format(id,self.port_of_switches_remained[dpid]))

    def arp_forward(self, sw, sw_port, msg):
        datapath = self.datapaths[sw]
        # ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        # self.logger.info("arp forward 发送到交换机{}的端口{}".format(datapath.id,sw_port))
        actions = [parser.OFPActionOutput(sw_port)]
        arp_message = _build_packet_out(datapath, actions, msg.data)
        datapath.send_msg(arp_message)

    def flood_arp(self, msg):
        # 发送packet_in 的交换机
        # parser = datapath.ofproto_parser
        # self.logger.info("flooding arp message")
        for dpid, ports in self.port_of_switches_remained.items():
            datapath = self.datapaths[dpid]
            parser = datapath.ofproto_parser
            for port in ports:
                actions = [parser.OFPActionOutput(port)]
                packet_arp = _build_packet_out(datapath, actions, msg.data)
                datapath.send_msg(packet_arp)


