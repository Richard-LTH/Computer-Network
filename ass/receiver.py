import sys
import socket
import time
from threading import Thread

#56007 59606 FileToReceive.txt 5000

#define the type of the segment
DATA = 0
ACK = 1
SYN = 2
FIN = 3

MSS = 1000
#define the states
CLOSED = 1
LISTEN = 2
ESTABLISHED = 3
TIME_WAIT = 4

BUF_SIZE = 1024


def increase_seqno(seqno, add):
    # cal_seqno
    seqno += add
    # check if the seq_no legal
    if seqno >= 2 ** 16:
        seqno = seqno - 2 ** 16
    return seqno


def update_receiver_state(new_state):
    global receiver_state
    receiver_state = new_state


def write_log(action, segment_type, seqno, number_bytes):
    global start_time
    # check if start_time is uninitialized
    if start_time == 0:
        # initialize start_time with current time
        start_time = time.time()
        current_time = 0
    else:
        # calculate time spend since start
        current_time = time.time() - start_time
    current_time = '{:.2f}'.format(current_time*1000)
    #content=f'{action}\t{current_time}\t{segment_type}\t{seqno}\t{number_bytes}
    log.write(f'{action}\t{current_time}\t{segment_type}\t{seqno}\t{number_bytes}\n')


def time_wait():
    time.sleep(2)
    # update receiver state
    update_receiver_state(CLOSED)


def start_receive():
    global sliding_window_seqno, dup_ack_segments_sent, dup_data_segments_received, original_data_received, original_segments_received
    # start listen
    update_receiver_state(LISTEN)
    # receive segments
    #3 types of segments
    while True:
        # check the receiver state
        if receiver_state == CLOSED:
            break
        try:
            segment, addr = receiver_socket.recvfrom(BUF_SIZE)
        # check timeout
        except socket.timeout:
            update_receiver_state(CLOSED)
            break
        # get type and seqno
        segment_type = int.from_bytes(segment[:2], byteorder='big')
        seqno = int.from_bytes(segment[2:4], byteorder='big')
        if segment_type == SYN:
            write_log('rcv', 'SYN', seqno, 0)
            ack_seqno = increase_seqno(seqno, 1)
            ack_segment = ACK.to_bytes(2, 'big')
            ack_segment += ack_seqno.to_bytes(2, 'big')
            receiver_socket.sendto(ack_segment, addr)
            write_log('snd', 'ACK', ack_seqno, 0)
            # update to ESTABLISHED
            if receiver_state == LISTEN:
                update_receiver_state(ESTABLISHED)
                sliding_window_seqno = increase_seqno(seqno, 1)
            else:
                dup_ack_segments_sent += 1
        #if data type
        elif segment_type == DATA:
            data = segment[4:]
            number_bytes = len(data)
            write_log('rcv', 'DATA', seqno, number_bytes)
            ack_seqno = increase_seqno(seqno, number_bytes)
            ack_segment = ACK.to_bytes(2, 'big')
            ack_segment += ack_seqno.to_bytes(2, 'big')
            receiver_socket.sendto(ack_segment, addr)
            write_log('snd', 'ACK', ack_seqno, 0)
            # check if the seqno of data already exists
            seqno_exists = False
            for entry in sliding_window:
                if entry is not None and entry['seqno'] == seqno:
                    seqno_exists = True
                    break
            if not seqno_exists:
                for entry in received_data[::-1]:
                    if entry['seqno'] == seqno:
                        seqno_exists = True
                        break
            if seqno_exists:
                #  count as duplicate data and ack
                dup_data_segments_received += 1
                dup_ack_segments_sent += 1
            else:
                # calculate insert index for the received data in the swindow
                if seqno < sliding_window_seqno:
                    seqno1 = seqno + 2 ** 16
                else:
                    seqno1 = seqno
                insert_index = (seqno1 - sliding_window_seqno) // MSS
                # extend window if needed
                if insert_index >= len(sliding_window):
                    sliding_window.extend([None for _ in range(insert_index - len(sliding_window) + 1)])
                # insert the received data in the window
                sliding_window[insert_index] = {'data': data, 'seqno': seqno}
                original_data_received += number_bytes
                original_segments_received += 1
                # move the data from window to list
                while True:
                    if len(sliding_window) == 0 or sliding_window[0] is None:
                        break
                    received_data.append(sliding_window[0])
                    sliding_window_seqno = increase_seqno(sliding_window_seqno, len(sliding_window[0]['data']))
                    # remove the first element from window
                    del sliding_window[0]
        #fin type
        else:
            write_log('rcv', 'FIN', seqno, 0)
            ack_seqno = increase_seqno(seqno, 1)
            ack_segment = ACK.to_bytes(2, 'big')
            ack_segment += ack_seqno.to_bytes(2, 'big')
            receiver_socket.sendto(ack_segment, addr)
            write_log('snd', 'ACK', ack_seqno, 0)
            if receiver_state == ESTABLISHED:
                update_receiver_state(TIME_WAIT)
            # set socket timeout to 2s
            receiver_socket.settimeout(2)
            #start thread
            Thread(target=time_wait).start()


if __name__ == '__main__':
    #read the commands
    receiver_port = int(sys.argv[1])
    sender_port = int(sys.argv[2])
    txt_file_received = sys.argv[3]
    max_win = int(sys.argv[4])
    #set up udp socket
    receiver_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    receiver_socket.bind(('', receiver_port))
    #define
    start_time = 0
    sliding_window = []
    sliding_window_seqno = 0
    received_data = []
    original_data_received = 0
    original_segments_received = 0
    dup_data_segments_received = 0
    dup_ack_segments_sent = 0
    receiver_state = CLOSED
    #record logfile
    log = open('Receiver_log.txt', 'w')
    #start receive
    print('Start Receive')
    start_receive()
    with open(txt_file_received, 'wb') as f:
        for entry in received_data:
            f.write(entry['data'])
    log.write(f'Original data received:     {original_data_received}\n')
    log.write(f'Original segments received: {original_segments_received}\n')
    log.write(f'Dup data segments received: {dup_data_segments_received}\n')
    log.write(f'Dup ack segments sent:      {dup_ack_segments_sent}\n')
    log.close()
