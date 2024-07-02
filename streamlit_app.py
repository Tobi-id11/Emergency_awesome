import streamlit as st
import pandas as pd
import psycopg2 as psql
import matplotlib.pyplot as plt
import seaborn as sns

# Function to connect to the PostgreSQL database and fetch data
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

# Streamlit app

st.title("Emergency Awesome Statistics")
st.write("This app displays all the information known about Emergency Awesome Youtube Channel.")
st.write("Emergency Awesome is a Youtube Content creator who focuses on movies and tv show reviews such as Marvel Movies,The Boys, House Of The Dragon Game Of Thrones and many more. ")

# Query to fetch all data
query = "SELECT * FROM student.tobi_df_capstone"
data = get_data_from_db(query)

# Display raw data
st.write("### Raw Data")
st.dataframe(data)

# Convert Published_date to datetime
data['published_date'] = pd.to_datetime(data['published_date'])

# Set up the matplotlib figure
sns.set(style="whitegrid")

# Top 10 most viewed videos
st.write("### Top 10 Most Viewed Videos")
top_10_viewed = data.nlargest(10, 'views')
plt.figure(figsize=(12, 6))
ax = sns.barplot(x='views', y='title', data=top_10_viewed, palette='viridis')
ax.set_title('Top 10 Most Viewed Videos')
st.pyplot(plt)
plt.clf()

# Top 10 most liked videos
st.write("### Top 10 Most Liked Videos")
top_10_liked = data.nlargest(10, 'likes')
plt.figure(figsize=(12, 6))
ax = sns.barplot(x='likes', y='title', data=top_10_liked, palette='magma')
ax.set_title('Top 10 Most Liked Videos')
st.pyplot(plt)
plt.clf()

# Top 10 videos with most comments
st.write("### Top 10 Videos with Most Comments")
top_10_comments = data.nlargest(10, 'comments')
plt.figure(figsize=(12, 6))
ax = sns.barplot(x='comments', y='title', data=top_10_comments, palette='coolwarm')
ax.set_title('Top 10 Videos with Most Comments')
st.pyplot(plt)
plt.clf()

# Latest video
st.write("### Latest Video")
latest_video = data.nlargest(1, 'published_date')
st.dataframe(latest_video)

# Oldest video
st.write("### Oldest Video")
oldest_video = data.nsmallest(1, 'published_date')
st.dataframe(oldest_video)

# Trend analysis for views over time
st.write("### Views Over Time")
plt.figure(figsize=(12, 6))
ax = sns.lineplot(x='published_date', y='views', data=data)
ax.set_title('Views Over Time')
plt.xticks(rotation=45)
st.pyplot(plt)
plt.clf()




