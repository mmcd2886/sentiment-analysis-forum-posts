import bs4 as bs
import pandas as pd
import requests
import nltk
import time
import pathlib
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, MetaData, Table, insert
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pandas import DataFrame


# Dataframes will be fully visible when printing
pd.set_option("display.max_rows", None, "display.max_columns", None, "display.max_colwidth", None)

# add custom sentiment for words
sid = SentimentIntensityAnalyzer()
new_words = {
    'hype': 2,
    'hyped': 2
}
sid.lexicon.update(new_words)

# User can enter which page to of thread to start collecting information on
url_link = input("Enter URL for first page of thread: ").rstrip()
file_name = input("Enter a name for the CSV file. Leave blank to not save: ").rstrip()

# User can enter which page of thread to start collecting information on
forum_thread_page_num = 1
starting_page = input("Enter number for which page of thread to start on. Leave blank to start on first page of "
                      "thread. ").rstrip()
if starting_page != '':
    starting_page = int(starting_page)
    forum_thread_page_num = starting_page

# User can enter which page of thread to stop collecting information on
ending_page = input(
    "Enter number for which page of thread to end on. Leave blank to end on last page of thread. ").rstrip()
if ending_page == '':
    ending_page = float('inf')
else:
    ending_page = int(ending_page)

# create the csv with headers if csv does not already exist
csv_file_path = pathlib.Path(file_name + '.csv')
if csv_file_path.exists():
    input(
        "This csv already exists. Press Enter to append existing csv or run script again and choose a different name.")
else:
    csv_header_df = pd.DataFrame(columns=['name', 'date_time', 'score', 'quote', 'sentiment', 'reply'])
    csv_header_df.to_csv(csv_file_path, index=False, mode="a", header=True, encoding='utf-8-sig')

replies_info_df = pd.DataFrame()

while True:
    time.sleep(0.5)
    url = url_link + 'page-' + str(forum_thread_page_num)
    print(forum_thread_page_num)

    # create bs object from the user entered url
    request = requests.get(url)
    response = request.text
    soup = bs.BeautifulSoup(response, 'lxml')

    # get the thread title
    thread_title_soup = soup.find("div", {"class": "p-title"})
    thread_title = thread_title_soup.get_text()
    thread_title = thread_title.replace('\n', ' ')

    # create list of usernames so that they can later be matched to comments
    username_list = []
    username = soup.findAll("a", {"itemprop": "name"})
    for name in username:
        new_name = name.get_text()
        username_list.append(new_name)

    # find the date & time that user's comments were posted
    date_soup = soup.findAll('div', {'class': 'message-attribution-main'})
    date_time_list = []
    # date and time is found in the '<time>' tag. In the time tag there is a variable called "datetime" that is = to
    # date of the post and formatted as '2020-09-24T18:28:56-0400', I store this datetime as a list object and slice it
    # so that I get only '2020-09-24 18:28:56 date_time.
    for item in date_soup:
        date_time = item.find('time').attrs['datetime'][0:19]
        date_time_list.append(date_time)

    # find the thread's comments
    comments = soup.findAll("div", {"class": "bbWrapper"})

    # Replies that quote other users will contain ("div", {"class": "bbCodeBlock-expandContent"}). If this is found
    # 'Quote' is added to the quote_reply_list Else: No Quote is added to list. To access comment, return only the
    # text from the comments. 'recursive=False' prevents parsing any sub-tags. All needed text is a direct child
    # of -> "div", {"class": "bbWrapper"}
    comment_list = []
    quote_reply_list = []
    for comment in comments:
        replies_to_quotes = comment.find_all("div", {"class": "bbCodeBlock-expandContent"})
        if len(replies_to_quotes) > 0:
            quoted_or_not = "quote"
        else:
            quoted_or_not = "No quote"
        quote_reply_list.append(quoted_or_not)

        comment = comment.find_all(text=True, recursive=False)
        comment = ''.join(comment)  # convert list to string
        comment = comment.replace('\n', ' ')  # remove new lines for paragraphs (combines multiple paragraphs to one)
        if not comment:  # if comment is empty
            comment = 'N/A'
        comment_list.append(comment)

    # use sentimentAnalyzer for each comment and create list of the 'compound' score for each comment
    compound_result_list = []
    for comment in comment_list:
        sentiment_result_dict = sid.polarity_scores(comment)
        compound_result = sentiment_result_dict.get('compound')
        compound_result_list.append(compound_result)

    # Iterate through the compound_result list to determine whether each score is pos. neut. or neg. Then append this
    # to the sentiment list
    sentiment_list = []
    for score in compound_result_list:
        if score <= -0.05:
            sentiment = "Negative"
        elif -0.05 < score < 0.05:
            sentiment = "Neutral"
        elif score >= 0.05:
            sentiment = "Positive"
        sentiment_list.append(sentiment)

    # combine five lists and convert to DataFrame
    new_dict = zip(username_list, date_time_list, compound_result_list, quote_reply_list, sentiment_list, comment_list)
    df = DataFrame(new_dict)
    replies_info_df = replies_info_df.append(df)

    # if there are less than 50 usernames it means it is the last page and should break
    if len(username_list) < 50 or forum_thread_page_num == ending_page:
        # Rename the columns of the dataframe
        replies_info_df.rename(columns={0: "username", 1: "date_time", 2: "score", 3: "quoted", 4: "sentiment", 5: "replies"},
                   inplace=True)
        # Convert date & time columns from string to datetime objects so that they can be manipulated with pandas
        replies_info_df["date_time"] = pd.to_datetime(replies_info_df["date_time"], format="%Y-%m-%d %H:%M:%S")

        print("\n--------------------\nTotal Replies: ", replies_info_df["username"].count(), "\n--------------------")

        # convert the first row of the dataframe to a dictionary and remove it from the dataframe. The first row of the
        # dataframe is the creator of the post and the first post of the thread. This dictionary will be uploaded to a
        # different table.
        first_comment_of_thread_dict = replies_info_df.to_dict('records')[0]
        pop_list = ['sentiment', 'quoted', 'score']
        for word in pop_list:
            first_comment_of_thread_dict.pop(word)
        first_comment_of_thread_dict.update({'url': url_link})
        first_comment_of_thread_dict.update({'title': thread_title})
        replies_info_df = replies_info_df.drop(replies_info_df.index[0])

        # ------Total Replies on a date in Chron order-------#
        # Groupby Day using freq='D' and then find the size() and add it to the column Total Replies which will tally
        # the total replies for each day. Convert the .size() series that it returns to a dataframe using .reset_index.
        # Convert DateTime object to just date using strftime. if there is only one date worth of replies,
        # use the if statement to group by hours instead to display hourly data for the single day. Else groupby day
        # and show daily Total Replies.
        df4 = replies_info_df.set_index("date_time").groupby(pd.Grouper(freq='D')).size().reset_index(name='total replies')
        df4["date_time"] = (df4["date_time"].dt.strftime('%Y-%m-%d'))
        # This date will be the x-axis title if there is only one day of Replies
        xlabel_date = str(df4["date_time"][0])
        if len(df4["date_time"]) == 1:
            df4 = replies_info_df.set_index("date_time").groupby(pd.Grouper(freq='H')).size().reset_index(name='total replies')
            df4["date_time"] = (df4["date_time"].dt.strftime('%H:%M:%S'))
            # I am calling .plot on the Dataframe using pandas. This method references the Matplot API
            df4.set_index("date_time").plot.bar(figsize=(13, 8))
            plt.xlabel(xlabel_date + " (Hourly)", fontsize=15)
            plt.title("total replies by hour", fontsize=30, color="Black")
        else:
            df4.set_index("date_time").plot.bar(figsize=(13, 8))
            plt.xlabel("Day", fontsize=15)
            plt.title("total replies by day", fontsize=30, color="Black")
        plt.ylabel("total replies", fontsize=15)
        plt.xticks(rotation=45)
        # limit the number of x axis labels using 'nbins='
        plt.locator_params(axis='x', nbins=13)
        plt.show()

        # ------Total Replies by a user-------#
        # Use groupby to group by Username, then use .size() to return a series that will show the total number for each
        # username. Convert this to a dataframe using .reset_index()
        total_replies_by_username_df = replies_info_df.groupby(["username"]).size().reset_index(name='total replies')
        # print number of unique users who posted in the thread
        print("\n---------------------------\nTotal Unique Replies: ", total_replies_by_username_df["username"].count(),
              "\n---------------------------")

        # Sort the dataframe users with least replies to greatest replies then get the tail which will have users with
        # the most posts. Do this because if the bar chart goes from greatest to least, the bar chart will be upside
        # down when plotted in matplot.
        total_replies_by_username_df.sort_values(by=['total replies'], inplace=True, ascending=True)
        total_replies_by_username_df = total_replies_by_username_df.tail(15)
        # matplot
        plt.figure(figsize=(13, 8))
        plt.barh(total_replies_by_username_df["username"], total_replies_by_username_df["total replies"])
        plt.xlabel("replies", fontsize=15)
        plt.ylabel("username", fontsize=15)
        plt.title("Total Replies by Username", fontsize=30)
        plt.show()

        # make everything lower case then tokenize
        lower_case_replies_info_df = replies_info_df['replies'].str.lower().str.cat(sep=' ')
        tokenized_replies_list = nltk.tokenize.word_tokenize(lower_case_replies_info_df)

        # remove stop words
        stop_words = set(stopwords.words('english'))
        replies_with_no_stop_words_list = []
        for word in tokenized_replies_list:
            if word not in stop_words:
                replies_with_no_stop_words_list.append(word)

        # remove punctuation
        cleaned_text_list = []
        for word in replies_with_no_stop_words_list:
            if word.isalpha():
                cleaned_text_list.append(word)
        word_dist = nltk.FreqDist(cleaned_text_list)

        # number of top words to display in matplot
        top_words = 50
        df_top_words = pd.DataFrame(word_dist.most_common(top_words), columns=['Word', 'Frequency'])

        # sort from least to greatest otherwise barchart will be upside down
        df_top_words.sort_values(by=['Frequency'], inplace=True, ascending=True)

        # create matplot for word frequency
        plt.figure(figsize=(12, 10))
        plt.barh(df_top_words["Word"], df_top_words["Frequency"])
        plt.xlabel("Word", fontsize=15)
        plt.ylabel("Frequency", fontsize=15)
        plt.title("Most Frequent Words", fontsize=30)
        plt.show()

        # --Sentiment with and without quotes matplot graph-- #
        # groupby the sentiment column (pos. neg. neut.) add up each and create the total sentiment column
        # Visualize Sentiment in pie chart
        sentiment_labels = ["Negative", "Neutural", "Positive"]
        sentiment_with_quotes_df = replies_info_df.groupby(["sentiment"]).size().reset_index(name="total sentiment")
        total_sentiment_with_quotes = sentiment_with_quotes_df["total sentiment"].tolist()
        plt.figure(figsize=(7, 7))
        plt.pie(total_sentiment_with_quotes, labels=sentiment_labels, explode=(0, 0.2, 0), shadow=True, startangle=90,
                autopct='%1.1f%%')
        plt.title("sentiment of all replies", fontsize=20)
        plt.show()
        # display(sentiment_with_quotes_df)

        # --Sentiment without quotes matplot graph-- #
        # groupby the sentiment column (pos. neg. neut.) add up each a create the total sentiment column, but do no
        # include replies that have quotes from other users in them because this could impact sentiment to the thread
        # title
        no_quotes_df = replies_info_df[replies_info_df["quoted"] == "No quote"]
        sentiment_no_quotes_df = no_quotes_df.groupby(["sentiment"]).size().reset_index(name="total sentiment")
        total_sentiment_no_quotes = sentiment_no_quotes_df["total sentiment"].tolist()
        plt.figure(figsize=(7, 7))
        plt.pie(total_sentiment_no_quotes, labels=sentiment_labels, explode=(0, 0.2, 0), shadow=True, startangle=90,
                autopct='%1.1f%%')
        plt.title("Sentiment of replies that do not contain quotes from other users", fontsize=20)
        plt.show()
        # display(sentiment_no_quotes_df)

        # Write dataFrame to csv in append mode with the header removed
        replies_info_df.to_csv(csv_file_path, index=False, mode="a", header=False, encoding='utf-8-sig')

        # Connect to SQLITE DB
        engine = create_engine('sqlite://///Users/untitled/pycharmProjects/forum-project/mysite/db.sqlite3')
        metadata = MetaData(bind=engine)
        connection = engine.connect()

        # insert url, thread title etc.. into polls_threads table
        table = Table('polls_threads', metadata, autoload=True, autoload_with=engine)
        insert_statement = insert(table).values(first_comment_of_thread_dict).prefix_with("OR IGNORE")
        results = connection.execute(insert_statement)

        # SELECT the primary key 'ID' from the polls_threads record that was just inserted to use as foreign key in
        # polls_posts
        url_id_query = connection.execute("SELECT id FROM polls_threads WHERE url = ?", url_link)
        id_num = url_id_query.fetchone()[0]

        # add the primary key from the above code to the dataframe which will be added to the polls_posts table. This
        # will serve as a foreign key to the polls_threads table
        replies_info_df.insert(0, 'thread_id', id_num)

        # insert username, replies ,etc..  into polls_posts table
        insert_values_forum_table = replies_info_df.to_dict(orient='records')
        table = Table('polls_posts', metadata, autoload=True, autoload_with=engine)
        insert_statement = insert(table).values(insert_values_forum_table).prefix_with("OR IGNORE")
        results = connection.execute(insert_statement)

        break
    else:
        forum_thread_page_num = forum_thread_page_num + 1
