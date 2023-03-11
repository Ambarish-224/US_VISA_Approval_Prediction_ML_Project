from flask import Flask
from visa.logger import logging

app = Flask(__name__)

@app.route('/', methods = ['GET', 'POST'])
def index():
    logging.info("WE are just testing logging module")
    return "Hello World"


if __name__=="__main__":
    app.run(debug=True)
