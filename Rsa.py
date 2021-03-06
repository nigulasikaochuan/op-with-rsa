import random
from math import ceil

from NetResource import NetResource
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.lib.packet import packet
from ryu.lib.packet import ipv4
from ryu.lib.packet import ethernet
from ryu.lib import hub


class Rsa(NetResource):
    def __init__(self, *args, **kwargs):
        super(Rsa, self).__init__(*args, **kwargs)
        self.modulation_format = {
            "BPSK": 25, "QPSK": 50, "8-QAM": 75, "16-QAM": 100
        }
        self.speed = None
        self.paths_between_node = {}

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        pkt = packet.Packet(msg.data)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            self.speed = random.choice([300, ])
            #   self.logger.info("ipv4 processing")
            self.shortest_forward(msg, ip_pkt, pkt)

        else:
            return

    def shortest_forward(self, msg, ip_pkt, pkt):
        datapaths = self.topo.datapaths
        ip_src = ip_pkt.src
        ip_dst = ip_pkt.dst
        src_id = None
        dst_id = None
        packet_in_datapath = msg.datapath
        packet_in_port = msg.match['in_port']
        e_type = pkt.get_protocol(ethernet.ethernet).ethertype
        # self.logger.info("发送ip包的是交换机{}：源ip地址是{}，目的ip地址是{}".format(packet_in_datapath.id, ip_src, ip_dst))
        hub.sleep(0.001)
        for (sw_id, sw_port), (host_ip, host_mac) in self.topo.HostSwitches.items():
            if ip_src == host_ip:
                src_id = sw_id
            if ip_dst == host_ip:
                dst_id = sw_id
            if src_id is not None and dst_id is not None:
                break
        if src_id != packet_in_datapath.id:
            '''
                源ip地址的switch id 和 packe_in 报文的switch id 不一样，直接还给交换机进行重新匹配
            '''
            # self.logger.info("src_id is {}".format(src_id))
            # self.logger.info("应该还给交换机{}，从这个交换机的port{}输入的".format(packet_in_datapath.id, msg.match['in_port']))
            ofproto = packet_in_datapath.ofproto
            parser = packet_in_datapath.ofproto_parser
            # hub.sleep(0.001)
            actions = [parser.OFPActionOutput(port=ofproto.OFPP_TABLE)]
            packet_out = self._build_packet_out(packet_in_datapath, actions=actions, data=msg.data,
                                                inport=msg.match['in_port'])
            packet_in_datapath.send_msg(packet_out)
            return

        if src_id is not None and dst_id is not None:
            paths = self.k_shortest_paths(src_id, dst_id, weight='weight', k=3)
            self.logger.info(paths)
            res, path, slot = self.do_assignment(paths)
            if res:

                # self.logger.info("path:{},slots{}".format(path, self.remainSlots))
                if path is not None:
                    self.paths_between_node[(ip_src, ip_dst)] = (path, slot)
                    flow_information = [e_type, ip_src, ip_dst, packet_in_port]
                    # self.logger.info("报文的信息是，以太网类型为{}，源{}目的{}，入端口{}".format(e_type, ip_src, ip_dst, packet_in_port))
                    self.install_flow(datapaths, path, flow_information, msg.buffer_id, data=msg.data)
                else:
                    self.logger.info("path 不可知")
            else:
                self.logger.info("please wait")
        else:

            self.logger.info("主机位置暂时不可知")
            self.logger.info("{}".format(self.topo.HostSwitches))

    def do_assignment(self, paths):
        for path in paths:
            distance = self.get_distance_of_path(path)
            mf = self.choose_mf(distance)

            slot_number = ceil(self.speed / self.modulation_format[mf])
            self.logger.info(
                "speed{},mf{},num{},dis{},path{}".format(self.speed, mf, slot_number, distance, path))
            res, slot = self.check_resource(slot_number, path)
            if res:
                self._creat_graph()

                return True, path, slot
            else:
                continue
        return False, None, None

    def get_distance_of_path(self, path):
        distance = 0
        for index, value in enumerate(path):
            if index < len(path) - 1:
                distance = distance + self.distance_between_nodes.get((path[index], path[index + 1]),
                                                                      0) + self.distance_between_nodes.get(
                    (path[index + 1], path[index]), 0)

        return distance

    def choose_mf(self, distance):
        if distance >= 3000:
            return "BPSK"
        if 1500 < distance < 3000:
            return "QPSK"
        if 700 < distance <= 1500:
            return "8-QAM"
        if distance <= 700:
            return "16-QAM"
        if distance < 0:
            raise ValueError

    def check_resource(self, slot_number, path):
        slot_can_be_used = self.get_slot_can_be_used(slot_number, path)
        slot_allocated = self.get_cs(slot_can_be_used, slot_number)

        if slot_allocated is None:
            return False, None
        # self.logger.info("slot_can_be used is{},con is {}".format(slot_can_be_used, slot_allocated))
        self.remove_res(path, slot_allocated)
        self._creat_graph()
        return True, slot_allocated

    def return_resouces(self, slot, path):
        for index, _ in enumerate(path):
            if index < len(path) - 1:
                remain_slot = self.remainSlots.get((path[index], path[index + 1]), None)
                if remain_slot is None:
                    remain_slot = self.remainSlots.get((path[index + 1], path[index]))
                remain_slot.extend(slot)

    def get_slot_can_be_used(self, slot_number, path):
        slot_can_be_used = set()
        for index, _ in enumerate(path):
            if index < len(path) - 1:
                remain_slot = self.remainSlots.get((path[index], path[index + 1]), None)
                if remain_slot is None:
                    remain_slot = self.remainSlots.get((path[index + 1], path[index]))
                    # print(path[index + 1], path[index])
                else:
                    pass
                    # print(path[index], path[index + 1])
                if len(remain_slot) < slot_number:
                    return False, None
                if slot_can_be_used:
                    slot_can_be_used = slot_can_be_used & set(remain_slot)
                else:
                    slot_can_be_used.update(set(remain_slot))
        return slot_can_be_used

    def get_cs(self, slot_can_be_used, slot_number):
        continue_slot_num = 0
        slot_allocated = []
        if len(slot_can_be_used) < slot_number:
            return None
        slot_can_be_used = sorted(slot_can_be_used)
        for index_, _ in enumerate(slot_can_be_used):
            if continue_slot_num == slot_number:
                break
            elif index_ < len(slot_can_be_used) - 1:
                if (slot_can_be_used[index_ + 1] - slot_can_be_used[index_]) > 1:
                    if continue_slot_num < slot_number:
                        continue_slot_num = 0
                        slot_allocated = []
                        continue
                elif (slot_can_be_used[index_ + 1] - slot_can_be_used[index_]) == 1:
                    if continue_slot_num == 0:
                        continue_slot_num += 2
                        slot_allocated.extend([slot_can_be_used[index_], slot_can_be_used[index_ + 1]])
                    else:
                        continue_slot_num += 1
                        slot_allocated.append(slot_can_be_used[index_ + 1])
        # 如果break 不执行else
        else:
            return None
        return slot_allocated

    def remove_res(self, path, slot_allocated):
        for index, _ in enumerate(path):

            if index < len(path) - 1:
                remain_slot = self.remainSlots.get((path[index], path[index + 1]), None)
                if remain_slot is None:
                    remain_slot = self.remainSlots.get((path[index + 1], path[index]))
                    # print(path[index + 1], path[index])
                else:
                    pass
                    # print(path[index], path[index + 1])
                # for i in slot_allocated:
                #     remain_slot.remove(i)
                for i in slot_allocated:
                    try:
                        remain_slot.remove(i)
                    except ValueError:
                        self.logger.info("error")
                        # self.logger.info(slot_can_be_used)
                        self.logger.info(remain_slot)
                        self.logger.info(index)
                        self.logger.info(i)
                        self.logger.info(slot_allocated)
        # self.logger.info("paths{}:{}".format(path, slot_allocated))
        # for i in slot_allocated:
        #     print(i,)

    @set_ev_cls(ofp_event.EventOFPFlowRemoved, MAIN_DISPATCHER)
    def remove_handler(self, ev):
        msg = ev.msg
        try:
            src_ip = msg.match["ipv4_src"]
            dst_ip = msg.match["ipv4_dst"]
            # self.logger.info("path_beween_nodes{}".format(self.paths_between_node))
            # for key in self.remainSlots:
            #     if len(self.remainSlots[key]) < 126:
            #         self.logger.info("false")
            #         break
            # else:
            #     self.logger.info("true")
            if (src_ip, dst_ip) in self.paths_between_node:
                path = self.paths_between_node[(src_ip, dst_ip)][0]
                slot = self.paths_between_node[(src_ip, dst_ip)][1]
                self.return_resouces(slot, path)

                self._creat_graph()
                del self.paths_between_node[(src_ip, dst_ip)]
            else:
                pass

        except Exception as e:
            pass
