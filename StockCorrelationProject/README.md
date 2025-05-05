

1) Clone the project repository to your local machine using Git
   git clone <repository_url>
   cd <repository_name>
2) Create and activate a Python virtual environment:
   python3 -m venv venv
   source venv/bin/activate
3) Install the required Python packages using pip:
   pip install -r requirements.txt
4)Ensure the data/stock_data.zip file exists in the data directory. This file should contain the stock data in CSV format.
5) Run the Streamlit application by entering `streamlit run app.py` in the terminal. 
   this will open up a browser window with the Streamlit app (usually http://localhost:8501).

If needed, update paths or settings in src/config/settings.py to match your environment.