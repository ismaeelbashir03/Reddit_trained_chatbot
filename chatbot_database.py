# we first import sqlite and json and date time
import sqlite3
import json
from datetime import datetime

# we make a variable for the time frame data we are using
timeframe = '2015-01' 

# we make a sql transaction to join all our sql quiries 
# together in one sql query which is way more efficient than 
# doing each comment one by one (there are millions)
sql_transaction = []

# here we connect to a database named after our time frame
connection = sqlite3.connect('{} db'.format(timeframe))

# defining our cursor
c = connection.cursor()


# here we create a function to create our table
def create_table():
    # we run our query here
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

# formatting our body text
def format_data(data):

    # we can take parts of the data we want to replace 
    # (double quotes -> single quotes, \n -> newlinechar, \r (return key) -> newlinechar)
    data = data.replace("\n", " NEWLINECHAR ").replace("\r", " NEWLINECHAR ").replace('"', "'")

    # we can now return our data
    return data

# we use this to find the parent of a comment, we do this by 
# using the parent id with each comment to do an sql query to get the
# comment which matches that parent id in the parent_reply table
def find_parent(parent_id):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(parent_id)
        
        # lets execute this query
        c.execute(sql)

        # we can get the result
        result = c.fetchone()

        # we can return the result if it exists
        if result != None:
            return result[0]

        else:
            return False

    except Exception as e:
        # print("find_parent: ", e)
        return False

def find_existing_score(parent_id):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(parent_id)
        
        # lets execute this query
        c.execute(sql)

        # we can get the result
        result = c.fetchone()

        # we can return the result if it exists
        if result != None:
            return result[0]

        else:
            return False

    except Exception as e:
        # print("find_score: ", e)
        return False

# here we create a function to find acceptable comments to train with
def acceptable(data):

    # we can check if there are more than 50 words or less than 1 (empty) words
    # we want to exclude these comments
    if len(data.split(" ")) > 50 or len(data) < 1:
        return False
    # if a word is more than 1000 characters we also want to exclude it
    elif len(data) > 1000:
        return False
    # if a comment was deleted or removed we want to exclude them aswell
    elif data == "[deleted]" or data == "[removed]":
        return False
    # if our comment made it through all these checks, then we will use it
    else:
        return True

def transaction_bldr(sql):
    # we first global the sql transaction to use it later in the code
    global sql_transaction

    # we append this sql statement to the transaction
    sql_transaction.append(sql)

    # we check if the transaction has reached a certain length
    if len(sql_transaction) > 1000:
        # we start a transaction, which runs multiple sql statements as once
        c.execute('BEGIN TRANSACTION')

        # we go through each sql statement
        for s in sql_transaction:
            # we try executing the statement in the transaction
            try:
                c.execute(s)

            # if not then we just ignore it
            except:
                pass
        
        # we can then commit the transaction to run all the queries
        connection.commit()

        # we then reset our transaction list
        sql_transaction = []
        
# using an sql query to uodate the parent_reply table at row of the parent id to be the new comment
def sql_insert_replace_comment(comment_id, parent_id, parent, comment, subreddit, time, score):
    try:
        sql = "UPDATE parent_reply SET parent_id = {}, comment_id = {}, parent = {}, comment = {}, subreddit = {}, unix = {}, score = {} WHERE parent_id ={};".format(parent_id, comment_id, parent, comment, subreddit, int(time), score, parent_id)
        # adding this sql statement to our transaction
        transaction_bldr(sql)

    except Exception as e:
        print("s-UPDATE insertion: ", str(e))

# inserting our comment in to the parent_reply table
def sql_insert_has_parent(comment_id, parent_id, parent, comment, subreddit, time, score):
    try:
        sql = "INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ('{}','{}','{}','{}','{}',{},{});".format(parent_id, comment_id, parent, comment, subreddit, int(time), score)
        # adding this sql statement to our transaction
        transaction_bldr(sql)

    except Exception as e:
        print('s-PARENT insertion: ',str(e))

# inserting our comment into the parent reply table even though it has no parent data, we do this if this
# is another comments parent
def sql_insert_no_parent(comment_id, parent_id, comment, subreddit, time, score):
    try:
        sql = "INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ('{}','{}','{}','{}',{},{});".format(parent_id, comment_id, comment, subreddit, int(time), score)
        # adding this sql statement to our transaction
        transaction_bldr(sql)

    except Exception as e:
        print('s-NO_PARENT insertion: ',str(e))

# if we are running through main
if __name__ == "__main__":

    # we create our table
    create_table()
    
    # we can create counters to check the number of rows and number of comments with replies
    row_counter = 0
    paired_rows = 0

    # we open our reddit data file
    with open("RC_2022-10", buffering=1000) as f: # with open("RC_2015-01", buffering=1000) as f:
        # we can loop through each row of data
        for row in f:

            # updating our counter
            row_counter += 1

            # reading in our data in json format
            row = json.loads(row)

            # assinging the variables that we want from row
            parent_id = row['parent_id']
            comment_id = row['name']
            body = format_data(row['body']) # we use a format function to filter
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            # we can filter this on scores (upvotes), score of 2 means 
            # at least 1 person upvotes
            if score >= 2:
                
                # we can check if there is a comment that has a greater score that was
                # in reply to our parent comment
                existing_comment_score = find_existing_score(parent_id)

                # we can check if the other comment score exists and checking if 
                # it is smaller than the score we have now
                if existing_comment_score:
                    if score > existing_comment_score:
                        # we can check if the comment is acceptable here so we dont have to run a 
                        # query for comments we wont even use
                        if acceptable(body):
                            # we can replace our previous comment replys
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                else:
                    # we can check if the comment is acceptable here so we dont have to run a 
                    # query for comments we wont even use
                    if acceptable(body):
                        # we check if this comment is a reply or parent
                        if parent_data:
                            # we can add it in as a reply
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            # we can add it in as a new comment (parent)
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

            # we can print our data every 100,000 rows
            if row_counter % 100000 == 0:
                print("total rows read: {} paired rows: {} time: {}".format(row_counter, paired_rows, str(datetime.now())))