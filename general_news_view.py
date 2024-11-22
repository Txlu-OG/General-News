import streamlit as st
import psycopg2 as psycopg
import pandas as pd

# Function to connect to CockroachDB
def init_conn():
    db_url = (
        'postgresql://localhost:26257/defaultdb?'
        'sslrootcert=C:/Dev/DAT608PROJECTS/cockroachdb/cockroachdb1/certs/ca.crt&'
        'sslkey=C:/Dev/DAT608PROJECTS/cockroachdb/cockroachdb1/certs/client.ukeme.key.pk8&'
        'sslcert=C:/Dev/DAT608PROJECTS/cockroachdb/cockroachdb1/certs/client.ukeme.crt&'
        'sslmode=verify-full&'
        'user=ukeme&password=cockroach'
    )
    conn = psycopg.connect(db_url, application_name="streamlit-app")
    return conn

# Fetch data from CockroachDB
def get_news_data(source=None):
    conn = init_conn()
    if source:
        query = "SELECT * FROM news_data WHERE source_name = %s LIMIT 100;"
        df = pd.read_sql_query(query, conn, params=(source,))
    else:
        query = "SELECT * FROM news_data LIMIT 100;"
        df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Streamlit app layout
st.title("News Data Viewer")
st.write("Displaying the latest news articles from the news_data table in CockroachDB.")

# Dropdown to select news source
source = st.selectbox("Select News Source", ["All", "Google News", "CNN News"])

# Fetch and display the news data
if source == "All":
    news_data = get_news_data()
else:
    news_data = get_news_data(source)

# Add a button to refresh the news data
if st.button("Refresh News"):
    st.session_state.news_data = news_data

# Display the news data
news_data = st.session_state.get('news_data', news_data)
st.dataframe(news_data)

# Optionally, add a chart or any other visualization
st.write("### Latest News")
for index, row in news_data.iterrows():
    st.write(f"**{row['title']}**")
    st.write(f"{row['description']}")
    st.write(f"[Read more]({row['url']})")
    st.write("---")
