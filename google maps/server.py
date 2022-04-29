import os
from flask import Flask
from flask import render_template
from flask import request, Response
from flask import g, redirect

app = Flask(__name__)

