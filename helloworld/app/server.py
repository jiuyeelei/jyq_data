# cat server.py 
from flask import Flask
from flask import request
import redis
import pymongo

r=redis.Redis(host='redis',port=6379)
r.delete('key')
r.set('key','kaixin001')
r.expire('key', 10)
redis_value = r.get('key')
print(redis_value)
#redis_value = "dongsheng"

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    return '<h1>Home %s</h1>' % redis_value

@app.route('/signin', methods=['GET'])
def signin_form():
    return '''<form action="/signin" method="post">
              <p><input name="username"></p>
              <p><input name="password" type="password"></p>
              <p><button type="submit">Sign In</button></p>
              </form>'''

@app.route('/signin', methods=['POST'])
def signin():
    if request.form['username']=='admin' and request.form['password']=='password':
        return '<h3>Hello, admin!</h3>'
    return '<h3>Bad username or password.</h3>'

if __name__ == '__main__':
    app.run('0.0.0.0',debug=True)
