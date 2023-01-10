# importing our libraries
import sqlite3
import pandas as pd

# we are using 2 timeframes of data, 2015 jan with around 2 million paired comments
# and 2022 october with {FILL IN AFTER MAKING DATABASE} million paired comments
timeframes = ['2015-01']

# we go through each time frame of data we have
for timeframe in timeframes:
    
    # we create a connection to that database
    connection = sqlite3.connect('{} db'.format(timeframe))
    # we get our cursor
    c = connection.cursor()

    # limiting our query result number of rows
    limit = 5000

    # initialising a last time variable to buffer through the database
    last_unix = 0

    # setting our current length to the limit
    cur_length = limit

    # initialising a counter
    counter = 0

    # creating a boolean to see if we are using our test file
    test_done = False
    
    # while we can pull data from our database
    while cur_length == limit:

        # using pandas we get the data that is more than the last unix time up to the limit size we also filter out comments with no replies
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection)
        
        # we update our last unix to the last unix pulled form the database
        last_unix = df.tail(1)['unix'].values[0]
        
        # updating our curr length
        cur_length = len(df)

        # we want to check if we are testing
        if not test_done:

            # here we write the comment and reply in parallel in two different files
            # e.g. from file line 1 question relates to to file line 1 reply

            # if so, we open our 'test.from' file with utf8 encoding (text) in append mode
            with open('test.from','a', encoding='utf8') as f:

                # we go through each parent value (question) in the pulled data 
                # from the database and write it to the file
                for content in df['parent'].values:
                    f.write(content+'\n')

            # if so, we open our 'test.to' file with utf8 encoding (text) in append mode
            with open('test.to','a', encoding='utf8') as f:
                
                # we go through each comment value (reply) in the pulled data 
                # from the database and write it to the file
                for content in df['comment'].values:
                    f.write(str(content)+'\n')

            # we finish writing to our test file, we only put the first pull in our test files
            test_done = True

        else:

            # here again we write the comment and reply in parallel in two different files
            # e.g. from file line 1 question relates to to file line 1 reply

            # if so, we open our 'train.from' file with utf8 encoding (emojis) in append mode
            with open('train.from','a', encoding='utf8') as f:
                
                # we go through each parent value (question) in the pulled data 
                # from the database and write it to the file
                for content in df['parent'].values:
                    f.write(content+'\n')

            # if so, we open our 'train.to' file with utf8 encoding (text) in append mode
            with open('train.to','a', encoding='utf8') as f:
                
                # we go through each comment value (reply) in the pulled data 
                # from the database and write it to the file
                for content in df['comment'].values:
                    f.write(str(content)+'\n')

        # we update our counter
        counter += 1

        # every 100,000 rows (20*5000) we will print an update
        if counter % 20 == 0:

            print(counter*limit,'rows completed so far')
