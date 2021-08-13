import bs4 as bs
import pandas as pd
import requests
from datetime import date
from datetime import datetime
import sqlite3
import nltk
import time
import pathlib
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, MetaData, Table, insert
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pandas import DataFrame

# nltk.download('vader_lexicon')
# nltk.download('punkt')
# nltk.download('stopwords')

# Dataframes will be fully visible when printing
pd.set_option("display.max_rows", None, "display.max_columns", None, "display.max_colwidth", None)

# add custom sentiment for words
sid = SentimentIntensityAnalyzer()
new_words = {
    'hype': 2,
    'hyped': 2
}
sid.lexicon.update(new_words)

# Connect to SQLITE DB
engine = create_engine('sqlite://///Users/default/PycharmProjects/django-forum/mysite/db.sqlite3')
metadata = MetaData(bind=engine)
connection = engine.connect()

"""
# User can enter any page of thread
url_link = input("Enter URL for thread: ").rstrip()
# strip everything after the last / of the URL so that you can append 'page-' to the url
split = url_link.rsplit("/", 1)
split_url = (split[0])
"""

# create bs object from the user entered url
request = requests.get("https://www.resetera.com/forums/gaming-forum.7/")
response = request.text
soup = bs.BeautifulSoup(response, 'lxml')

# create a list of urls for the threads. exclude the pinned threads. To include pinned threads,
# thread_url_soup needs to find a class that includes all of the threads. The rest of the logic
# in this block should be able to remain the same.
thread_url_list = []
thread_url_soup = soup.find("div", {"class": "structItemContainer-group js-threadList"})
for tag in thread_url_soup.find_all("div", {"class": "structItem-title"}):
    for url in tag.find_all('a', href=True):
        thread_url_list.append('https://www.resetera.com' + url['href'])

# Find the total number of views and replies for a thread. exclude pinned threads. To include pinned threads,
# total_thread_views_and_replies_soup needs to find a class that includes all of the threads. The rest of the logic
# in this block should be able to remain the same.
total_thread_views_and_replies_list = []
total_thread_views_and_replies_soup = soup.find("div", {"class": "structItemContainer-group js-threadList"})
for tag in total_thread_views_and_replies_soup.find_all("li", {"class": "uix_threadRepliesMobile"}):
    total_thread_views_and_replies = tag.find('dd')
    if total_thread_views_and_replies is not None:
        total_thread_views_and_replies_list.append(total_thread_views_and_replies.get_text())

# numbers in thousands will have commas e.g. 30,231. Remove commas to make data easier to analyze
total_thread_views_and_replies_no_commas_list = []
for reply_view in total_thread_views_and_replies_list:
    reply_view_remove_comma = reply_view.replace(',', '')
    total_thread_views_and_replies_no_commas_list.append(reply_view_remove_comma)

# in the total_thread_views_and_replies_list_no_commas_list, view totals and reply totals alternate. Views start at
# index 0, replies start at index 1. Get every other element to create the two lists below. e.g. list[0::2],
# 0 is the starting index, and 2 gets every other element.
total_thread_views_list = total_thread_views_and_replies_no_commas_list[0::2]
total_thread_replies_list = total_thread_views_and_replies_no_commas_list[1::2]

# this loop will iterate through the url for the thread, total views for the thread, and total replies for the thread
for base_url, total_views, total_replies in zip(thread_url_list, total_thread_views_list, total_thread_replies_list):
    print(base_url)

    # https://stackoverflow.com/questions/36439032/how-do-you-pass-through-a-python-variable-into-sqlite3-query
    # Query the last scraped page number using the URL in the posts_threads table. This will be the URL that scraping
    # starts on so scrapes are not duplicated. If URL has already been scraped, update the last_date_scraped to today's
    # date, update total_views, total_replies, and reply_rate_percentage
    last_scraped_page_query = connection.execute("SELECT last_page_scraped FROM polls_threads WHERE url = ?", base_url)
    try:
        forum_thread_page_num = int(last_scraped_page_query.fetchone()[0])
        url_has_already_been_scraped = "Yes"
        todays_date = datetime.now()
        update_last_date_scrape = connection.execute("UPDATE polls_threads SET last_date_scraped = ?, total_views = ?, total_replies = ? where url = ?",
                                                     todays_date, total_views, total_replies, base_url)
        print("Furthest page scraped is: ", forum_thread_page_num)
    except TypeError:
        url_has_already_been_scraped = "No"
        forum_thread_page_num = 1
        print("This url has not been scraped yet")


    """
    file_name = input("Enter a name for the CSV file. Leave blank to not save: ").rstrip()

    # User can enter which page of thread to start collecting information on
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
    """

    replies_info_df = pd.DataFrame()

    while_loop_iterator = 0
    while True:
        time.sleep(0.5)
        url = base_url + 'page-' + str(forum_thread_page_num)

        # create bs object from the user entered url
        request = requests.get(url)
        response = request.text
        soup = bs.BeautifulSoup(response, 'lxml')

        # Find the thread page-number UI navigation on the page. The last number on the navigation is the last page
        # of the thread. if it cannot find the page-nav it means it does not exist and there is only one page for the
        # thread. If it finds the page-nav it will find the text (page numbers) and store the last one. Have to use [
        # -2] because the last element is empty for some reason. Only need to get the last page number of thread
        # once, so use while_loop_iterator to only get it once during the first while loop iteration.
        if while_loop_iterator == 0:
            last_page_of_thread_soup = soup.find("ul", {"class": "pageNav-main"})
            if last_page_of_thread_soup is None:
                last_page_of_thread = 1
            else:
                last_page_of_thread = int((list(last_page_of_thread_soup)[-2].get_text()))
        while_loop_iterator = 1

        print("Page:", forum_thread_page_num, "out of", last_page_of_thread)
        # Get the thread title only when scraping the first page. Don't need to rescrape the title for every page.
        if forum_thread_page_num == 1:
            thread_title_soup = soup.find("div", {"class": "p-title"})
            thread_title = thread_title_soup.get_text()
            thread_title = thread_title.replace('\n', ' ')

        # create list of usernames so that they can later be matched to replies
        username_list = []
        username_soup = soup.find_all("h4", {"class": "message-name"})
        for username in username_soup:
            username_list.append(username.text)

        # find the date & time that user's replies were posted
        date_soup = soup.findAll("ul", {'class': 'message-attribution-main listInline'})
        date_time_list = []
        # date and time is found in the '<time>' tag. In the time tag there is a variable called "datetime" that is =
        # to date of the post and formatted as '2020-09-24T18:28:56-0400', I store this datetime as a list object and
        # slice it so that I get only '2020-09-24 18:28:56' date_time.
        for item in date_soup:
            date_time = item.find('time').attrs['datetime'][0:19]
            date_time_list.append(date_time)

        # find the thread's replies
        replies_soup = soup.findAll("div", {"class": "bbWrapper"})

        # Replies that quote other users will contain ("div", {"class": "bbCodeBlock-expandContent"}). If this is found
        # 'Quote' is added to the quote_reply_list Else: No Quote is added to list. To access reply, return only the
        # text from the replies. 'recursive=False' prevents parsing any sub-tags. All needed text is a direct child
        # of -> "div", {"class": "bbWrapper"}
        replies_list = []
        quote_reply_list = []
        for reply in replies_soup:
            replies_to_quotes = reply.find_all("div", {"class": "bbCodeBlock-expandContent"})
            if len(replies_to_quotes) > 0:
                quoted_or_not = "quote"
            else:
                quoted_or_not = "No quote"
            quote_reply_list.append(quoted_or_not)

            reply = reply.find_all(text=True, recursive=False)
            reply = ''.join(reply)  # convert list to string
            reply = reply.replace('\n', ' ')  # remove new lines for paragraphs (combines multiple paragraphs to one)
            if not reply:  # if reply contains no text by the user. E.g. only contains a quote or meme
                reply = 'N/A'
            replies_list.append(reply)

        # use sentimentAnalyzer for each reply and create list of the 'compound' score for each reply
        compound_result_list = []
        for reply in replies_list:
            sentiment_result_dict = sid.polarity_scores(reply)
            compound_result = sentiment_result_dict.get('compound')
            compound_result_list.append(compound_result)

        # Iterate through the compound_result list to determine whether each score is pos. neut. or neg. Then append
        # this to the sentiment list
        sentiment_list = []
        for score in compound_result_list:
            if score <= -0.05:
                sentiment = "Negative"
            elif -0.05 < score < 0.05:
                sentiment = "Neutral"
            elif score >= 0.05:
                sentiment = "Positive"
            sentiment_list.append(sentiment)

        # create a list that contains the page number for the current page of the thread.
        # this number will be added to replies table in DB to indicate which page of the thread a reply occured on.
        amount_of_replies_on_page = len(username_list)
        page_of_thread_list = [forum_thread_page_num] * amount_of_replies_on_page
        # combine five lists and convert to DataFrame
        replies_zipped_dict = zip(username_list, date_time_list, compound_result_list, quote_reply_list, sentiment_list,
                                  replies_list, page_of_thread_list)
        replies_zipped_df = DataFrame(replies_zipped_dict)
        replies_info_df = replies_info_df.append(replies_zipped_df)

        # if statement will equate to TRUE when on the last page of the thread.
        if forum_thread_page_num == last_page_of_thread:
            # Rename the columns of the dataframe
            replies_info_df = replies_info_df.rename(columns={0: "username", 1: "date_time", 2: "score", 3: "quoted",
                                                              4: "sentiment", 5: "replies", 6: "thread_page"})

            # Convert date & time columns from string to datetime objects so that they can be manipulated with pandas
            replies_info_df["date_time"] = pd.to_datetime(replies_info_df["date_time"], format="%Y-%m-%d %H:%M:%S")

            # If the this URL has already been scraped, there is no need to add the thread info to the db again. If it
            # has not already been scraped, convert add the first row of the replies_info_df to a dictionary and
            # remove it from the dataframe. The first row of the dataframe is the creator of the post and the first
            # post of the thread. This dictionary will be uploaded to the threads table.
            if url_has_already_been_scraped == "No":
                thread_info_dict = replies_info_df.to_dict('records')[0]
                pop_list = ['sentiment', 'quoted', 'score', 'thread_page']
                for word in pop_list:
                    thread_info_dict.pop(word)
                thread_info_dict.update({'url': base_url})
                thread_info_dict.update({'title': thread_title})
                thread_info_dict.update({'last_page_scraped': forum_thread_page_num})
                thread_info_dict.update({'total_views': total_views})
                todays_date = datetime.now()
                thread_info_dict.update({'last_date_scraped': todays_date})
                replies_info_df = replies_info_df.drop(replies_info_df.index[0])

                # insert url, thread title, etc.. into polls_threads table. 'OR IGNORE' will ignore if record exists
                threads_table = Table('polls_threads', metadata, autoload=True, autoload_with=engine)
                threads_insert_statement = insert(threads_table).values(thread_info_dict)
                execute_threads_insert_statement = connection.execute(threads_insert_statement)

            """
            # ------Total Replies on a date in Chron order-------#
            # Groupby Day using freq='D' and then find the size() and add it to the column Total Replies which will tally
            # the total replies for each day. Convert the .size() series that it returns to a dataframe using .reset_index.
            # Convert DateTime object to just date using strftime. if there is only one date worth of replies,
            # use the if statement to group by hours instead to display hourly data for the single day. Else groupby day
            # and show daily Total Replies.
            total_posts_datetime_df = replies_info_df.set_index("date_time").groupby(pd.Grouper(freq='D')).size().reset_index(name='total replies')
            total_posts_datetime_df["date_time"] = (total_posts_datetime_df["date_time"].dt.strftime('%Y-%m-%d'))
            # This date will be the x-axis title if there is only one day of Replies
            xlabel_date = str(total_posts_datetime_df["date_time"][0])
            if len(total_posts_datetime_df["date_time"]) == 1:
                total_posts_datetime_df = replies_info_df.set_index("date_time").groupby(pd.Grouper(freq='H')).size().reset_index(name='total replies')
                total_posts_datetime_df["date_time"] = (total_posts_datetime_df["date_time"].dt.strftime('%H:%M:%S'))
                # I am calling .plot on the Dataframe using pandas. This method references the Matplot API
                total_posts_datetime_df.set_index("date_time").plot.bar(figsize=(13, 8))
                plt.xlabel(xlabel_date + " (Hourly)", fontsize=15)
                plt.title("total replies by hour", fontsize=30, color="Black")
            else:
                total_posts_datetime_df.set_index("date_time").plot.bar(figsize=(13, 8))
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
            sentiment_labels = ["Negative", "Neutral", "Positive"]
            sentiment_with_quotes_df = replies_info_df.groupby(["sentiment"]).size().reset_index(name="total sentiment")
            total_sentiment_with_quotes = sentiment_with_quotes_df["total sentiment"].tolist()
            plt.figure(figsize=(7, 7))
            plt.pie(total_sentiment_with_quotes, labels=sentiment_labels, explode=(0, 0.2, 0), shadow=True, startangle=90,
                    autopct='%1.1f%%')
            plt.title("sentiment of all replies", fontsize=20)
            plt.show()
            # display(sentiment_with_quotes_df)

            # --Sentiment without quotes matplot graph-- #
            # groupby the sentiment column (pos. neg. neut.) add up each a create the total sentiment column, but do not
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
            """

            # SELECT the primary key 'thread_id' from the polls_threads record for the URL to use as foreign key in
            # polls_posts
            url_id_query = connection.execute("SELECT thread_id FROM polls_threads WHERE url = ?", base_url)
            thread_id_num = url_id_query.fetchone()[0]

            # add the 'thread_id' primary key from the above code to the dataframe which will be added to the
            # polls_posts table 'thread_id' column. This will serve as a foreign key to the polls_threads table
            replies_info_df.insert(0, 'thread_id', thread_id_num)

            # insert usernames, replies, reply sentiment, time of reply dict. into polls_posts table. I defined a
            # composite key for this table this will prevent duplicate records from being added to the DB; 'OR
            # IGNORE' will will skip records that already exist.
            replies_info_dict = replies_info_df.to_dict(orient='records')
            posts_table = Table('polls_posts', metadata, autoload=True, autoload_with=engine)
            posts_insert_statement = insert(posts_table).values(replies_info_dict).prefix_with("OR IGNORE")
            execute_posts_insert_statement = connection.execute(posts_insert_statement)

            # Count the total number of distinct usernames replies then update this value in the thread table.
            total_distinct_usernames = connection.execute("SELECT COUNT(DISTINCT username) as usernames FROM polls_posts WHERE thread_id = ?", thread_id_num).fetchone()[0]
            update_total_distinct_usernames = connection.execute("UPDATE polls_threads SET total_distinct_usernames = ? where thread_id = ?", total_distinct_usernames, thread_id_num)

            # This code is duplicated
            # count the total number of replies the update this value in the thread table
            total_thread_replies = connection.execute("SELECT COUNT(replies) FROM polls_posts WHERE thread_id = ?", thread_id_num).fetchone()[0]
            update_total_thread_replies = connection.execute("UPDATE polls_threads SET total_replies = ? where thread_id = ?", total_thread_replies, thread_id_num)

            # This code is duplicated
            # update the reply percentage
            # calculate what percentage of people who viewed the thread also replied to the thread.
            reply_rate_percentage = int(total_distinct_usernames) / int(total_views) * 100
            reply_rate_percentage_rounded = round(reply_rate_percentage, 1)
            update_reply_rate_percentage = connection.execute("UPDATE polls_threads SET reply_rate_percentage = ? where thread_id = ?", reply_rate_percentage_rounded, thread_id_num)

            # calculate the percent of replies that are written by distinct usernames i.e. of the replies, how many of them were made by unique usernames
            percent_distinct_replies = int(total_distinct_usernames) / int(total_thread_replies) * 100
            percent_distinct_replies_rounded = round(percent_distinct_replies, 1)
            update_percent_distinct_replies = connection.execute("UPDATE polls_threads SET percent_distinct_replies = ? where thread_id = ?", percent_distinct_replies_rounded, thread_id_num)


            # close the db connection to prevent a database error
            # connection.close()
            print("Finished\n")
            break
        else:
            forum_thread_page_num = forum_thread_page_num + 1
