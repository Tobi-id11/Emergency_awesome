import streamlit as st
import pandas as pd
import psycopg2 as psql
import matplotlib.pyplot as plt
import seaborn as sns


# Function to connect to the database and grab the data
def get_data_from_db(query):
    conn = psql.connect(
        database=st.secrets["postgresql"]["database"],
        user=st.secrets["postgresql"]["user"],
        host=st.secrets["postgresql"]["host"],
        password=st.secrets["postgresql"]["password"],
        port=5432
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Page for the Streamlit app

st.title("Emergency Awesome Dashboard")
channel_icon_url = "https://yt3.googleusercontent.com/VFO3dq0dA2UC7qjyPeT8A9i-uREXjqBYRFQfV1ZBaR4AAeutLMRbco15M50b_1S1nntawcs=w1707-fcrop64=1,00005a57ffffa5a8-k-c0xffffffff-no-nd-rj"
st.image(channel_icon_url, width=750)
st.write("This is the YouTube Statistic dashboard for Emergency Awesome. ")
st.write("A well-known YouTuber, Emergency Awesome is renowned for providing insightful evaluations and analysis of films and television series. Charlie Schneider is the channel's owner, and it has a sizable fan base thanks to its intelligent analysis, in-depth dissections, and interesting content." )


# Query to retrieve all the data
query = "SELECT * FROM student.tobi_df_capstone"
data = get_data_from_db(query)


# Display Playlist Page
st.write("### Playlist")
st.write("This is the full playlist of Emergency awesome Youtube channel.")
st.dataframe(data)


# Set up the matplotlib figure
sns.set(style="whitegrid")

# Top 10 most viewed videos
st.write("### Top 10 Most Viewed Videos")
top_10_viewed = data.nlargest(10, 'views')
plt.figure(figsize=(12, 6), dpi=100)
ax = sns.barplot(x='views', y='title', data=top_10_viewed, palette='coolwarm')
ax.set_title('Top 10 Most Viewed Videos')
st.pyplot(plt)
plt.clf()

most_viewed_video = data.nlargest(1, 'views')
most_viewed_title = most_viewed_video.iloc[0]['title']
most_video_count = most_viewed_video.iloc[0]['views']
st.write(f" The most viewed video on the channel is {most_viewed_title} with currently {most_video_count} views.")

# Top 10 most liked videos
st.write("### Top 10 Most Liked Videos")
top_10_liked = data.nlargest(10, 'likes')
plt.figure(figsize=(12, 6), dpi=100)
ax = sns.barplot(x='likes', y='title', data=top_10_liked, palette='coolwarm')
ax.set_title('Top 10 Most Liked Videos')
st.pyplot(plt)
plt.clf()

most_liked_video = data.nlargest(1, 'likes')
most_liked_title = most_liked_video.iloc[0]['title']
most_video_liked_amount = most_liked_video.iloc[0]['likes']
st.write(f" The most liked video on the channel is {most_liked_title} with currently {most_video_liked_amount} likes.")

# Top 10 videos with most comments
st.write("### Top 10 Videos with Most Comments")
top_10_comments = data.nlargest(10, 'comments')
plt.figure(figsize=(12, 6), dpi=100)
ax = sns.barplot(x='comments', y='title', data=top_10_comments, palette='coolwarm')
ax.set_title('Top 10 Videos with Most Comments')
st.pyplot(plt)
plt.clf()

most_commented_video = data.nlargest(1, 'likes')
most_commented_title = most_commented_video.iloc[0]['title']
most_video_comment_amount = most_commented_video.iloc[0]['likes']
st.write(f" The most commented video on the channel is {most_commented_title} with currently {most_video_comment_amount} comments.")


# Latest video
st.write("### Latest Video")
latest_video = data.nlargest(1, 'published_date')
st.dataframe(latest_video)

# Oldest video
st.write("### Oldest Video")
oldest_video = data.nsmallest(1, 'published_date')
st.dataframe(oldest_video)

# views over the period of his YouTube channel
st.write("### Views Over Time")
plt.figure(figsize=(12, 6), dpi=100)
ax = sns.lineplot(x='published_date', y='views', data=data)
ax.set_title('Views Over Time')
plt.xticks(rotation=45)
st.pyplot(plt)
plt.clf()


