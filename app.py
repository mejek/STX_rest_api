from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_
import requests
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///cz_books.db'
# app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://vnuktceybqbfou:20467ae50504e401004dacdd3234af2eb248edccfaa07fd2' \
#                                         'c9c953f522489e06@ec2-44-205-41-76.compute-1.amazonaws.com:5432/d9hcm5cnhp6dl4'
app.config['JSON_SORT_KEYS'] = False  # wyświetlanie wyniku zgodnie z kolejnością kolumn w bazie
db = SQLAlchemy(app)


class Book(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    external_id = db.Column(db.String(), unique=True)
    title = db.Column(db.String())
    authors = db.Column(db.String())
    acquired = db.Column(db.Boolean)
    published_year = db.Column(db.String(4))
    thumbnail = db.Column(db.String())


@app.route('/')
def index():
    return 'Hello STX NEXT :)'


@app.route('/api_spec', methods=['GET'])
def api_spec():
    api_spec_info = {"info": {"version": "2022.05.16"}}
    return api_spec_info


@app.route('/books')
def get_books():
    filters = []
    if request.args.get('title') is not None:
        filters.append(Book.title.ilike(f'%{request.args.get("title")}%')) # ilike - case insensitive
    if request.args.get('author') is not None:
        filters.append(Book.authors.ilike(f'%{request.args.get("author")}%'))
    # niektóre dane published_year mają postąc ze znakiem ? - filtr nie działa poprawnie na te pozycje
    if request.args.get('From') is not None:
        filters.append(Book.published_year >= str(request.args.get('From')))
    if request.args.get('to') is not None:
        filters.append(Book.published_year <= str(request.args.get('to')))
    if request.args.get('acquired') is not None:
        if request.args.get('acquired').lower() == 'false':
            filters.append(Book.acquired == False)
        elif request.args.get('acquired').lower() == 'true':
            filters.append(Book.acquired == True)

    books = Book.query.filter(and_(*filters)).all()
    output = []
    for book in books:
        book_data = {'id': book.id,
                     'external_id': book.external_id,
                     'title': book.title,
                     'authors': json.loads(book.authors),
                     'acquired': book.acquired,
                     'published_year': book.published_year,
                     'thumbnail': book.thumbnail}
        output.append(book_data)
    return jsonify(output)


@app.route('/books/<book_id>')
def get_book(book_id):
    book = Book.query.get_or_404(book_id)
    book_data = {'id': book.id,
                 'external_id': book.external_id,
                 'title': book.title,
                 'authors': json.loads(book.authors),
                 'acquired': book.acquired,
                 'published_year': book.published_year,
                 'thumbnail': book.thumbnail}
    return book_data


@app.route('/books/<book_id>', methods=['DELETE'])
def delete_book(book_id):
    book = Book.query.get_or_404(book_id)
    db.session.delete(book)
    db.session.commit()
    return f'Book id: {book_id} has been deleted'


@app.route('/books/<book_id>', methods=['PATCH'])
def update_book(book_id):
    updated_arguments = request.json.keys()

    for arg in updated_arguments:
        Book.query.filter(Book.id == book_id).update({arg: request.json[arg]})
    db.session.commit()

    return get_book(book_id)


@app.route('/books', methods=['POST'])
def add_book():
    data_keys = ['external_id', 'title', 'authors', 'published_year', 'acquired', 'thumbnail']
    request_keys_list = list(request.json.keys())

    if sorted(data_keys) == sorted(request_keys_list):
        book_data = Book(external_id=request.json['external_id'],
                         title=request.json['title'],
                         authors=json.dumps(request.json['authors']),
                         published_year=request.json['published_year'],
                         acquired=request.json['acquired'],
                         thumbnail=request.json['thumbnail'])

        db.session.add(book_data)
        db.session.commit()
        new_book = Book.query.order_by(Book.id.desc()).first()
        book_data = {'id': new_book.id,
                     'external_id': new_book.external_id,
                     'title': new_book.title,
                     'authors': json.loads(new_book.authors),
                     'acquired': new_book.acquired,
                     'published_year': new_book.published_year,
                     'thumbnail': new_book.thumbnail}
        return book_data
    else:
        return 'Wrong BODY format in POST Method.'


# IMPORT KSIĄŻEK Z GOOGLEAPIS
@app.route('/import', methods=['POST'])
def add_books():
    nazwisko = request.json['author']
    books_data = get_data_from_googleapis(nazwisko)
    duplicate_count = 0

    for book in books_data:
        # sprawdzenie czy książka nie istnieje już w bazie
        exists = Book.query.filter_by(external_id=book['external_id']).first()

        if not exists:
            new_book = Book(external_id=book['external_id'],
                            title=book['title'],
                            authors=json.dumps(book['authors']),
                            acquired=False,
                            published_year=book['published_year'],
                            thumbnail=book['thumbnail'])
            db.session.add(new_book)
            db.session.commit()
        else:
            duplicate_count += 1
    result = {'imported': len(books_data) - duplicate_count}
    return result


def get_totalItems_count(nazwisko):
    req_text = f'https://www.googleapis.com/books/v1/volumes?q=inauthor:{nazwisko}&maxResults=1'
    respond = requests.get(req_text)
    totalItems = respond.json()['totalItems']
    # print(f'Total items: {totalItems}')
    return totalItems


def get_data_from_googleapis(nazwisko):
    totalItems = get_totalItems_count(nazwisko)
    item_index = 0
    books_data = []  # lista książek do importu do bazy

    while item_index < totalItems:
        req_text = f'https://www.googleapis.com/books/v1/volumes?q=' \
                   f'inauthor:{nazwisko}&startIndex={item_index}&maxResults=40'
        respond = requests.get(req_text)
        if 'items' not in respond.json().keys():
            item_index += 1
            continue
        for data in respond.json()['items']:
            book_data = {}  # dane książki do importu do bazy
            book_data['external_id'] = data['id']
            book_data['title'] = data['volumeInfo']['title']
            if 'authors' in data['volumeInfo'].keys():
                book_data['authors'] = data['volumeInfo']['authors']
            else:
                item_index += 1
                continue
            if 'publishedDate' in data['volumeInfo'].keys():
                if data['volumeInfo']['publishedDate'][:4].isdigit():
                    book_data['published_year'] = data['volumeInfo']['publishedDate'][:4]
                else:
                    book_data['published_year'] = None
            else:
                book_data['published_year'] = None
            if 'imageLinks' in data['volumeInfo'].keys():
                book_data['thumbnail'] = data['volumeInfo']['imageLinks']['thumbnail']
            else:
                book_data['thumbnail'] = None
            books_data.append(book_data)
            print(item_index, book_data)

            item_index += 1
    return books_data

# deployment
# github


if __name__ == '__main__':
    app.run()
