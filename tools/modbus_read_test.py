import socket
import struct

HOST = "192.168.1.104"
PORT = 1502


def recv_exact(sock, size):
    """确保从TCP连接中读取指定字节数。"""
    data = b""

    while len(data) < size:
        chunk = sock.recv(size - len(data))

        if not chunk:
            raise ConnectionError("连接提前关闭，未收到完整响应")

        data += chunk

    return data


def main():
    transaction_id = 1
    unit_id = 1

    # Modbus功能码03：读取保持寄存器
    # 起始地址0，连续读取10个寄存器
    pdu = struct.pack(">BHH", 3, 0, 10)

    # Modbus TCP报文头：事务ID、协议ID、长度、设备地址
    mbap = struct.pack(
        ">HHHB",
        transaction_id,
        0,
        len(pdu) + 1,
        unit_id,
    )

    print(f"正在连接 {HOST}:{PORT} ...")

    with socket.create_connection((HOST, PORT), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(mbap + pdu)

        # MBAP报文头共7字节
        header = recv_exact(sock, 7)
        tid, protocol_id, length, response_unit = struct.unpack(
            ">HHHB", header
        )

        # 长度字段包含1字节设备地址，因此剩余PDU长度为length-1
        response_pdu = recv_exact(sock, length - 1)

    if tid != transaction_id or protocol_id != 0:
        raise RuntimeError("响应报文头不正确")

    if response_unit != unit_id:
        raise RuntimeError("响应设备地址不匹配")

    if not response_pdu:
        raise RuntimeError("收到空响应")

    if response_pdu[0] & 0x80:
        exception_code = response_pdu[1]
        raise RuntimeError(
            f"Modbus异常响应，异常码={exception_code}"
        )

    if response_pdu[0] != 3:
        raise RuntimeError("返回的功能码不是03")

    byte_count = response_pdu[1]

    if byte_count != 20 or len(response_pdu) != 2 + byte_count:
        raise RuntimeError("寄存器数据长度不正确")

    registers = list(
        struct.unpack(">" + "H" * (byte_count // 2),
                       response_pdu[2:])
    )

    print("\n========== Modbus读取成功 ==========")
    print("10个寄存器：", registers)
    print(f"温度：{registers[0] / 10:.1f} ℃")
    print(f"湿度：{registers[1] / 10:.1f} %")
    print(f"开关状态：{registers[2]}")
    print(f"设备计数器：{(registers[3] << 16) | registers[4]}")
    print(f"设备状态：{registers[5]}")
    print("扩展数据：", registers[6:10])


if __name__ == "__main__":
    main()
