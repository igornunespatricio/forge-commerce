import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('/home/jovyan/.env')

# jupyter_notebook_config.py
c.ServerApp.token = ''
c.ServerApp.password = ''
c.ServerApp.disable_check_xsrf = True
c.ServerApp.allow_origin = '*'
c.ServerApp.allow_remote_access = True
c.ServerApp.open_browser = False
c.ServerApp.ip = '0.0.0.0'
