import sys
import socket
import time
import struct
import threading

sendingLock = threading.Lock()
window = {}
closeFlag = True
maxSeqNum = -1

#Thread that reads the file continuously
class fileReader(threading.Thread):
	def __init__(self, cmdInput, cSock, receiver):
		threading.Thread.__init__(self)
		self.host = cmdInput[0]				#SERVER IP ADDRESS
		self.port = int(cmdInput[1])		#SERVER PORT
		self.file = cmdInput[2]				#FILE TO TRANSMIT
		self.n    = int(cmdInput[3])		#WINDOW SIZE
		self.MSS  = int(cmdInput[4])		#MAXIMUM SEGMENT SIZE
		self.sock = cSock
		self.r = receiver
		self.start()		
		
	def run(self):
		self.rdt_send()	
		
	def rdt_send(self):
		global window
		global maxSeqNum

		fileHandle = open(self.file,'rb')
		currSeq = 0
		sendMsg = ''
		
		b = True
		while b:
			b = fileHandle.read(1)
			sendMsg += str(b,'UTF-8')
			if len(sendMsg) == self.MSS or (not b):		
				while len(window) >= self.n:
					pass
				sender(self.sock, self.host, self.port, sendMsg, currSeq)    #Thread spawned to handle a single packet
				currSeq += 1
				sendMsg = ''
						
		sendMsg = '00000end11111'
		sender(self.sock, self.host, self.port, sendMsg,currSeq)		#Thread spawned to send the end packet
		maxSeqNum = currSeq
		fileHandle.close()

		
#Thread Class to handle the sending of a single packet
class sender(threading.Thread):
	def __init__(self, cSock, hst, prt, msg, s):
		threading.Thread.__init__(self)
		self.data = msg				#DATA OF 1 MSS SIZE TO BE SENT
		self.seqNum = s				#SEQUENCE NUMBER OF THE PACKET
		self.sock = cSock
		self.host = hst				#SERVER IP ADDRESS
		self.port = prt				#SERVER PORT
		self.start()
	
	def computeChecksum(self, data):
		sum = 0
		for i in range(0, len(data), 2):
			if i+1 < len(data):
				data16 = ord(data[i]) + (ord(data[i+1]) << 8)		#To take 16 bits at a time
				interSum = sum + data16
				sum = (interSum & 0xffff) + (interSum >> 16)		#'&' to ensure 16 bits are returned
		return ~sum & 0xffff										#'&' to ensure 16 bits are returned
				
	def formPacket(self, data, seq):
		#32 bit sequence number
		#16 bit check of the data part
		#16 bit 0101010101010101 -- Indicates data packet(in int 21845)
		seqNum = struct.pack('=I',seq)
		checksum = struct.pack('=H',self.computeChecksum(data))		#Computes the checksum of data
		dataIndicator = struct.pack('=H',21845)
		packet = seqNum+checksum+dataIndicator+bytes(data,'UTF-8')
		return packet
	
	def run(self):
		global window
		global sendingLock
		global closeFlag
		 
		sendingLock.acquire()
		packet = self.formPacket(self.data, self.seqNum)				#Packets are created here
		#window[self.seqNum] = (time.time(), 0)
		window[self.seqNum] = time.time()
		self.sock.sendto(packet,(self.host, self.port))
		#print('Sent '+str(self.seqNum))
		sendingLock.release()
		try:
			#while window[self.seqNum][1] <= 0:
			while self.seqNum in window:
				sendingLock.acquire()
				#if  time.time() - window[self.seqNum][0] < 1:									#RETRANSMISSION time = 5 seconds
				if self.seqNum in window: 
					if time.time() - window[self.seqNum] <= 0.2:									#RETRANSMISSION time = 5 seconds
						pass
					else:					
						print('TIMEOUT, SEQUENCE NUMBER = '+str(self.seqNum))
						self.sock.sendto(packet,(self.host, self.port))	#RETRANSMISSION of time-out packets(No ACK Received)					
						#window[self.seqNum] = (time.time(), 0)	
						window[self.seqNum] = time.time()
				sendingLock.release()
					
			#del window[self.seqNum]
			if self.seqNum == maxSeqNum:
				closeFlag = False
		except:
			print('Server closed its connection - Sender')
			self.sock.close()
		
		
		
		
#Thread Class to receive the ACK Packets from the Server
class receiver(threading.Thread):
	def __init__(self, cmdInput, cSock):		
		threading.Thread.__init__(self)
		self.host = cmdInput[0]
		self.port = int(cmdInput[1])
		self.file = cmdInput[2]
		self.n    = int(cmdInput[3])
		self.MSS  = int(cmdInput[4])
		self.sockAddr = cSock
		self.start()
	
	def parseMsg(self, msg):
		sequenceNum = struct.unpack('=I', msg[0:4])			#Sequence Number Acked by the server
		zero16 = struct.unpack('=H', msg[4:6])				#16 bit field with all 0's
		identifier = struct.unpack('=H', msg[6:])			#16 bit field to identify the ACK packets
		return sequenceNum, zero16, identifier
		
	def run(self):
		print('Receiver Spawned')
		global sendingLock	
		global window
		global closeFlag

		closeFlag = True
		try:
			while closeFlag == True or len(window) > 0:			
				ackReceived, server_addr = self.sockAddr.recvfrom(2048)			#Receives the ACK packets 
				sequenceNum , zero16, identifier = self.parseMsg(ackReceived)
				
				if int(zero16[0]) > 0:
					print('Receiver Terminated')
					break
					
				#16 bit identifier field to identify the ACK packets - 1010101010101010 [in int 43690]		
				#if int(identifier[0]) == 43690 and int(sequenceNum[0]) in window:
				if int(identifier[0]) == 43690:
					#print('ACKED :'+str(sequenceNum[0]))
					sendingLock.acquire()
					del window[int(sequenceNum[0])]
					#window[int(sequenceNum[0])] = (window[int(sequenceNum[0])][0], 1)					
					sendingLock.release()			
		except:
			print('Server closed its connection - Receiver')
			self.sockAddr.close()

			
def main():
	host = sys.argv[1]
	port = int(sys.argv[2])
	cliSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)	
	cliSocket.bind(('',3334)) 
	
	startTime = time.time()
	ackReceiver = receiver(sys.argv[1:], cliSocket)					#Thread that receives ACKs from the Server
	fileHandler = fileReader(sys.argv[1:],cliSocket, ackReceiver) 	#Thread that reads the file and sending of packets
	fileHandler.join() 			#Main thread waits till the sender finishes
	ackReceiver.join()			#Main thread waits till the ACK receiver finishes
	endTime = time.time()
	
	print('Total Time Taken:'+str(endTime-startTime))
	if cliSocket:
		cliSocket.close()
		
	
if __name__ == '__main__':	
	main()	


