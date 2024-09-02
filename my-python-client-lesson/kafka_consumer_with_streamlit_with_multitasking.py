'''
This module illustrates the use of Python threads to handle different
Consumers. Each Consumer will run in its own thread.
The threads will communicate with the main application thread via Queue.
This is because the streamlit runs only in the main process and 
needs to be updated with the data from Consumers
'''
import json
import logging
from kafka import KafkaConsumer
import streamlit as st
# Each consumer will run in its own thread
from threading import Thread
from queue import Queue 
import atexit # To help us listen for application shutdown and use the opportunity to clean

# Define consume function that will be passed to each thread
# Parameters include: consumer (to be run in the thread), threadName (for identity purpose) and q (an instance of python Queue)
def consume(consumer, threadName, q, format):
    for msg in consumer:
        #decode the message from the bytearray. This is case of string
        if format == 'string':
            data = (msg.value).decode()
        else:
            data = msg.value
        displayItem = {"threadName": threadName, "data": "Event picked by {}: '{}'".format(threadName, data)}
        #logging.info("Adding displayItem {}".format(displayItem))
        q.put(displayItem) # Put the message in the Queue

consumer1 = KafkaConsumer('quickstart-events', auto_offset_reset='earliest')
consumer2 = KafkaConsumer('s1',value_deserializer=lambda m: json.loads(m.decode('utf8')), auto_offset_reset='earliest')
consumer3 = KafkaConsumer('S1_TRANSFORMED',value_deserializer=lambda m: json.loads(m.decode('utf8')), auto_offset_reset='earliest')

consumer1ThreadName = "CONSUMER ONE"
consumer2ThreadName = "CONSUMER TWO"
consumer3ThreadName = "CONSUMER THREE"

#Python program entry point
if __name__ == "__main__":
    
    # Function to be called when before exiting. Optional
    '''
    def exit_handler():
        logging.info('Application is ending!')
        logging.info("Main    : wait for the thread to finish")
        consume1.join(3)
        consume2.join(3)
        consume3.join(3)
        logging.info("Main    : all done")
        exit(0)
    
    atexit.register(exit_handler) # Optional
    '''
    try:
        st.set_page_config(layout="wide")
        st.title('Visualizing Streams')
        container1 = st.container()
        container2 = st.container(border=True)

        with container1:
            # Setup two columns for displaying content
            col1, col2 = st.columns(2, gap="small")
            with col1:
                st.subheader(consumer1ThreadName)
                stDisplayConsume1 = st.empty() # create an empty space to receive content
            with col2:
                st.subheader(consumer2ThreadName)
                stDisplayConsume2 = st.empty() # create an empty space to receive content

        with container2:
            st.subheader(consumer3ThreadName)
            stDisplayConsume3 = st.empty()

        # Create the shared queue
        q = Queue()

        #Initialize the texts for display in the two columns
        displayConsume1Text = ''
        displayConsume2Text = ''
        displayConsume3Text = ''

        #Format logging
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
        logging.info("Main: before creating thread")

        #declare consume1 thread
        consume1 = Thread(target=consume, args=(consumer1,consumer1ThreadName, q, "string"))
        logging.info("Main: before running thread")
        consume1.start() #start the thread

        #declare consume2 thread
        consume2 = Thread(target=consume, args=(consumer2,consumer2ThreadName, q, "json"))
        logging.info("Main: before running thread")
        consume2.start() #start the thread

        #declare consume3 thread
        consume3 = Thread(target=consume, args=(consumer3,consumer3ThreadName, q, "json"))
        logging.info("Main: before running thread")
        consume3.start() #start the thread

        #Enter an infinite loop and keep reading the Queue and acting
        while True:
            #Get content to display from queue
            displayItem = q.get() #Main thread subscribes to the Queue

            threadName = displayItem["threadName"]
            data = "{}\n".format(displayItem["data"])
            
            if(threadName == consumer1ThreadName):
                displayConsume1Text += data #Concatenate to the display text
                stDisplayConsume1.text(displayConsume1Text)
            elif(threadName == consumer2ThreadName):
                displayConsume2Text += data
                stDisplayConsume2.text(displayConsume2Text)
            else:
                displayConsume3Text += data
                stDisplayConsume3.text(displayConsume3Text)

    except Exception as e:
        logging.error("Fatal Error encountered: {}".format(e))
        exit(1)


