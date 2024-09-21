import random
import sys
import socket
import time
from threading import Thread


#59606 56007 asyoulik.txt 5000 100 0.1 0.1

#define the type of the segment
DATA = 0
ACK = 1
SYN = 2
FIN = 3

MSS = 1000
#define the states
CLOSED = 1
SYN_SENT = 2
ESTABLISHED = 3
CLOSING = 4
FIN_WAIT = 5

BUF_SIZE = 1024


def increase_seqno(seqno, add):
    # increase the sequence number
    seqno += add

    # check if the sequence number is legal
    if seqno >= 2 ** 16:
        seqno = seqno - 2 ** 16
    return seqno


def update_sender_state(new_state):
    global sender_state
    sender_state = new_state


def write_log(action, segment_type, seqno, number_bytes):
    global start_time
    # check if start_time is uninitialized
    if start_time == 0:
        # initialize start_time with current time
        start_time = time.time()
        current_time = 0
    else:
        # calculate time spend since start_time
        current_time = time.time() - start_time
    current_time = '{:.2f}'.format(current_time * 1000)
    log.write(f'{action}\t{current_time}\t{segment_type}\t{seqno}\t{number_bytes}\n')


def read_txt():
    with open(txt_file_to_send, 'r') as f:
        all_data = f.read().encode('utf-8')

    #initialize sequence number with the ACK sequence number for SYN
    seqno = ack_syn_seqno
    i = 0
    data_segments = []
    while i < len(all_data):
        data = all_data[i:i + MSS]
        data_segments.append({'data': data, 'seqno': seqno, 'received_ack': False, 'last_sent_time': 0.0})
        i += MSS
        seqno=increase_seqno(seqno,MSS)
    return data_segments


def retransmit_syn_thread():
    global last_syn_sent_time
    # loop for transmitting SYN segments
    while True:
        # time remaining for rto
        seconds = rto - (time.time() - last_syn_sent_time)
        if seconds > 0:
            time.sleep(seconds)
        # break the loop if SYN have been sent
        if sender_state != SYN_SENT:
            break
        syn_segment = SYN.to_bytes(2, 'big')
        syn_segment += syn_seqno.to_bytes(2, 'big')
        if random.random() < flp:
            write_log('drp', 'SYN', syn_seqno, 0)
        else:
            sender_socket.sendto(syn_segment, ('127.0.0.1', receiver_port))
            write_log('snd', 'SYN', syn_seqno, 0)
        # Update the last SYN sent time to the current time
        last_syn_sent_time = time.time()


def retransmit_fin_thread():
    global last_fin_sent_time
    # loop for transmitting FIN segments
    while True:
        # time remaining for rto
        seconds = rto - (time.time() - last_fin_sent_time)
        if seconds > 0:
            time.sleep(seconds)
        #  if sender state changes from FIN_WAIT
        if sender_state != FIN_WAIT:
            break
        fin_segment = FIN.to_bytes(2, 'big')
        fin_segment += fin_seqno.to_bytes(2, 'big')
        if random.random() < flp:
            write_log('drp', 'FIN', fin_seqno, 0)
        else:
            sender_socket.sendto(fin_segment, ('127.0.0.1', receiver_port))
            write_log('snd', 'FIN', fin_seqno, 0)
        # update the last FIN sent time to the current time
        last_fin_sent_time = time.time()


def retransmit_data_thread():
    global retransmitted_segments, data_segments_dropped
    while True:
        # check sender state
        if sender_state not in [ESTABLISHED, CLOSING]:
            break
        # find the first unacknowledged segment in the sliding window
        first_unacked = None
        for data in sliding_window:
            if data['received_ack'] is False and data['last_sent_time'] != 0:
                first_unacked = data
                break
        # calculate time remaining for rto of the unacknowledged segment
        if first_unacked is not None:
            seconds = rto - (time.time() - first_unacked['last_sent_time'])
            if seconds > 0:
                time.sleep(seconds)
            # if the segment is still unacknowledged after rto, retransmit it
            if first_unacked['received_ack'] is False:
                if random.random() < flp:
                    write_log('drp', 'DATA', first_unacked['seqno'], len(first_unacked['data']))
                    data_segments_dropped += 1
                    #print('data dropped')
                else:
                    data_segment = DATA.to_bytes(2, 'big')
                    data_segment += first_unacked['seqno'].to_bytes(2, 'big')
                    data_segment += first_unacked['data']
                    sender_socket.sendto(data_segment, ('127.0.0.1', receiver_port))
                    #print('data sent')
                    write_log('snd', 'DATA', first_unacked['seqno'], len(first_unacked['data']))
                retransmitted_segments += 1
                first_unacked['last_sent_time'] = time.time()


if __name__ == '__main__':
    #read the command
    sender_port = int(sys.argv[1])
    receiver_port = int(sys.argv[2])
    txt_file_to_send = sys.argv[3]
    max_win = int(sys.argv[4]) // MSS
    rto = int(sys.argv[5]) / 1000
    flp = float(sys.argv[6])
    rlp = float(sys.argv[7])
    #setup udp socket
    sender_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    sender_socket.bind(('', sender_port))
    #define
    start_time = 0
    last_syn_sent_time = 0.0
    last_fin_sent_time = 0.0
    syn_seqno = random.randint(0, 2 ** 16 - 1)
    ack_syn_seqno = increase_seqno(syn_seqno, 1)
    data_segments = read_txt()
    last_data_segment = data_segments[-1]
    fin_seqno = increase_seqno(last_data_segment['seqno'], len(last_data_segment['data']))
    ack_fin_seqno = increase_seqno(fin_seqno, 1)
    sliding_window = []
    sender_state = CLOSED

    original_data_sent = 0
    original_data_acked = 0
    original_segments_sent = 0
    retransmitted_segments = 0
    dup_acks_received = 0
    data_segments_dropped = 0
    ack_segments_dropped = 0
    #open log file
    log = open('Sender_log.txt', 'w')

    # establishing connection
    syn_segment = SYN.to_bytes(2, 'big')
    syn_segment += syn_seqno.to_bytes(2, 'big')
    if random.random() < flp:#dont send
        print('SYN DOES NOT SEND')
        write_log('drp', 'SYN', syn_seqno, 0)
    else:
        #send
        sender_socket.sendto(syn_segment, ('127.0.0.1', receiver_port))
        print('SYN SEND')
        write_log('snd', 'SYN', syn_seqno, 0)
    sender_state = SYN_SENT
    last_syn_sent_time = time.time()
    # start thread
    Thread(target=retransmit_syn_thread, daemon=True).start()

    # waiting for SYN-ACK:
    while True:
        segment, addr = sender_socket.recvfrom(BUF_SIZE)
        segment_type = int.from_bytes(segment[:2], byteorder='big')
        seqno = int.from_bytes(segment[2:4], byteorder='big')
        if random.random() < rlp:
            write_log('drp', 'ACK', seqno, 0)
            ack_segments_dropped += 1
            print('ACK SEGMENT DROPPED')
            continue
        write_log('rcv', 'ACK', seqno, 0)
        # break loop if established
        #ack
        if seqno == ack_syn_seqno:
            if sender_state == SYN_SENT:
                sender_state = ESTABLISHED
                print('SYN ACK')
                print('Established Connection.')
                break
    # sending data:
    print('Starting Send Data')
    # fill window until window is full or empty
    while len(sliding_window) < max_win:
        if len(data_segments) == 0:
            break
        data = data_segments[0]
        del data_segments[0]
        sliding_window.append(data)
        if random.random() < flp:
            write_log('drp', 'DATA', data['seqno'], len(data['data']))
            data_segments_dropped += 1
        else:
            data_segment = DATA.to_bytes(2, 'big')
            data_segment += data['seqno'].to_bytes(2, 'big')
            data_segment += data['data']
            sender_socket.sendto(data_segment, ('127.0.0.1', receiver_port))
            write_log('snd', 'DATA', data['seqno'], len(data['data']))
        data['last_sent_time'] = time.time()
        original_data_sent += len(data['data'])
        original_segments_sent += 1
    # start thread
    Thread(target=retransmit_data_thread, daemon=True).start()

    # receiving ACKs and Handling Sliding Window:
    while True:
        segment, addr = sender_socket.recvfrom(BUF_SIZE)
        segment_type = int.from_bytes(segment[:2], byteorder='big')
        seqno = int.from_bytes(segment[2:4], byteorder='big')

        if random.random() < rlp:#drop
            write_log('drp', 'ACK', seqno, 0)
            ack_segments_dropped += 1
            continue

        write_log('rcv', 'ACK', seqno, 0)#receive
        # find the acked segment in the window
        current_acked_data = None
        for data in sliding_window:
            if increase_seqno(data['seqno'], len(data['data'])) == seqno:
                current_acked_data = data
                break
        if current_acked_data is None:
            dup_acks_received += 1
            continue
        if current_acked_data['received_ack']:
            dup_acks_received += 1
        else:
            original_data_acked += 1
            current_acked_data['received_ack'] = True

            # remove acked segments from the sliding window
            while len(sliding_window) > 0 and sliding_window[0]['received_ack']:
                del sliding_window[0]

            # add new segments to the
            while len(sliding_window) < max_win and len(data_segments) > 0:
                data = data_segments[0]
                del data_segments[0]
                sliding_window.append(data)
                if random.random() < flp:
                    write_log('drp', 'DATA', data['seqno'], len(data['data']))
                    data_segments_dropped += 1
                else:
                    data_segment = DATA.to_bytes(2, 'big')
                    data_segment += data['seqno'].to_bytes(2, 'big')
                    data_segment += data['data']
                    sender_socket.sendto(data_segment, ('127.0.0.1', receiver_port))
                    write_log('snd', 'DATA', data['seqno'], len(data['data']))
                data['last_sent_time'] = time.time()
                original_data_sent += len(data['data'])
                original_segments_sent += 1


            # turn to CLOSING state if data send over
            if sender_state == ESTABLISHED and len(data_segments) == 0:
                sender_state = CLOSING

                print('Data Send Over.')

            # turn to FIN_WAIT state
            if sender_state == CLOSING and len(sliding_window) == 0:
                sender_state = FIN_WAIT
                fin_segment = FIN.to_bytes(2, 'big')
                fin_segment += fin_seqno.to_bytes(2, 'big')
                if random.random() < flp:
                    write_log('drp', 'FIN', fin_seqno, 0)
                    print('FIN SEGMENT DROPPED')#FIN DROP
                else:
                    sender_socket.sendto(fin_segment, ('127.0.0.1', receiver_port))#send FIN
                    print('FIN SEND')
                    write_log('snd', 'FIN', fin_seqno, 0)
                last_fin_sent_time = time.time()
                # start thread
                Thread(target=retransmit_fin_thread, daemon=True).start()
                break

    # waiting for FIN-ACK:
    while True:
        segment, addr = sender_socket.recvfrom(BUF_SIZE)
        segment_type = int.from_bytes(segment[:2], byteorder='big')
        seqno = int.from_bytes(segment[2:4], byteorder='big')
        if random.random() < rlp:
            write_log('drp', 'ACK', seqno, 0)
            print('FINACK SEGMENT DROPPED')
            ack_segments_dropped += 1
            continue
        write_log('rcv', 'ACK', seqno, 0)
        print('FINACK')
        if seqno == ack_fin_seqno:
            sender_state = CLOSED
            break

    sender_socket.close()

    log.write(f'Original data sent:     {original_data_sent}\n')
    log.write(f'Original data acked:    {original_data_acked}\n')
    log.write(f'Original segments sent: {original_segments_sent}\n')
    log.write(f'Retransmitted segments: {retransmitted_segments}\n')
    log.write(f'Dup acks received:      {dup_acks_received}\n')
    log.write(f'Data segments dropped:  {data_segments_dropped}\n')
    log.write(f'Ack segments dropped:   {ack_segments_dropped}\n')
    log.close()

