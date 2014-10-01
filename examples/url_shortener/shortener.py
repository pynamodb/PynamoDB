"""
A fully working url shortener example
"""
from __future__ import print_function
import flask
from hashlib import md5
from base64 import b64encode
from pynamodb.models import Model
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
from pynamodb.attributes import UnicodeAttribute


class Url(Model):
    class Meta:
        table_name = "shortened-urls"
        host = "http://localhost:8000"

    class CodeIndex(GlobalSecondaryIndex):
        class Meta:
            read_capacity_units = 1
            write_capacity_units = 1
            projection = AllProjection()
        code = UnicodeAttribute(hash_key=True)

    url = UnicodeAttribute(hash_key=True)
    code = UnicodeAttribute()
    code_index = CodeIndex()

    def save(self, **kwargs):
        """
        Generates the shortened code before saving
        """
        self.code = b64encode(
            md5(self.url.encode('utf-8')).hexdigest()[-4:].encode('utf-8')
        ).decode('utf-8').replace('=', '').replace('/', '_')
        super(Url, self).save(**kwargs)

app = flask.Flask(__name__)
app.config.update(DEBUG=True)


@app.route('/')
def index():
    return flask.render_template("index.html")


@app.route('/shorten/<path:url>')
def shorten(url):
    model = Url(url)
    model.save()
    return flask.Response(model.code)


@app.route('/<path:code>')
def resolve(code):
    # next() in Python3 is __next__()
    try:
        model = Url.code_index.query(code).next()
    except AttributeError:
        model = Url.code_index.query(code).__next__()
    except StopIteration:
        flask.abort(404)
    finally:
        return flask.redirect(model.url)

if __name__ == "__main__":
    if not Url.exists():
        print("Creating table...")
        Url.create_table(wait=True, read_capacity_units=1, write_capacity_units=1)
    app.run()
