def add_miss_flow(datapath):
    '''
    用来下发miss-flow流表项目
    :param datapath:
    :return:
    '''

    ofproto = datapath.ofproto
    parser = datapath.ofproto_parser
    actions = [parser.OFPActionOutput(port=ofproto.OFPP_CONTROLLER, max_len=ofproto.OFPCML_MAX)]
    send_flow_mod(datapath, actions, pri=0, match=parser.OFPMatch())


def _build_packet_out_2(datapath, buffer_id, dst_port, data):
    """
            Build packet out object.
    """
    ofproto = datapath.ofproto
    actions = []
    if dst_port:
        actions.append(datapath.ofproto_parser.OFPActionOutput(dst_port))

        msg_data = None
        if buffer_id == datapath.ofproto.OFP_NO_BUFFER:
            if data is None:
                return None
            msg_data = data
        # packet_out 的 inport有什么用。。。。

        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=buffer_id,
            data=msg_data, in_port=ofproto.OFPP_CONTROLLER, actions=actions)
        return out


def _build_packet_out(datapath, actions, data):
    '''

          :param datapath:这个packout消息要发送到的交换机
          :param actions: 动作必须是可迭代的
          :param data: packetout消息要封装的数据
          :return: 无

          构造packet_out报文
    '''

    ofproto = datapath.ofproto
    parser = datapath.ofproto_parser
    # actions = [parser.OFPActionOutput(port)]
    packet_out_msg = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER,
                                         in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=data)
    return packet_out_msg


def send_flow_mod(datapath, actions, pri, match, idle_time=0, hard_time=0):
    '''
    下发正常流表的函数
    :param datapath: 要将流表发送到的交换机
    :param actions: instructions将要执行的动作
    :param pri: 这个流表的优先级
    :param match: 流表的匹配域
    :param idle_time: 空闲失效时间 默认为0 表示一直不失效
    :param hard_time: 没有合适的中文翻译。
    :return: None
    '''
    # self.logger.info("anzhuangliubiao")
    # if pri == 1:
    #     self.logger.info("addliubiao")
    parser = datapath.ofproto_parser
    ofproto = datapath.ofproto
    ins = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
    message = parser.OFPFlowMod(datapath=datapath, command=ofproto.OFPFC_ADD, priority=pri, match=match,
                                idle_timeout=idle_time, hard_timeout=hard_time, instructions=ins,
                                flags=ofproto.OFPFF_SEND_FLOW_REM)

    datapath.send_msg(message)
