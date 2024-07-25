import streamlit as st

if __name__ == '__main__':
    # Set the configuration for the Streamlit page
    st.set_page_config(
        page_title="Home"
    )
    
    # Set the main title of the page
    st.title("All things Franchise Cricket")
    
    # Set the header for the "About" section
    st.header("About") 
    
    # Write a description about the app and its purpose
    st.write("""
            As a big fan of Franchise Cricket, it always bothered me how much attention the Indian Premier League (IPL) gets, compared to other leagues.
             
            So, I've created this web app to blend my passion for Cricket with Data Engineering.
             
            Here, you can stay updated on news, podcasts, standings, and squads (coming up) from Franchise Cricket leagues around the world.

            Plus, building this app has helped me learn some Data Engineering tools and techniques along the way.
            """)
    
    # Provide a link to the GitHub repository of the project
    st.write("[GitHub](https://github.com/nitheeshkoushik/franchiseCric)")
