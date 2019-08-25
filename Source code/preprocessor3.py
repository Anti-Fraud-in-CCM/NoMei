from nltk import word_tokenize
from nltk import pos_tag
from allennlp.models.archival import load_archive
from allennlp.predictors import Predictor
from allennlp.predictors.constituency_parser import ConstituencyParserPredictor
from allennlp.predictors.sentence_tagger import SentenceTaggerPredictor
import MySQLdb


def nltk_tag(message):
    words = word_tokenize(message)
    message = ' '.join(words)
    message = message.replace('< file_name >', '<file_name>').replace('< url >', '<url>').replace('< version >', '<version>')
    words = message.split(' ')

    tags = pos_tag(words)
    tokens = [tag[0] for tag in tags]
    tags = [tag[1] for tag in tags]
    tokens = ' '.join(tokens)
    tags = ' '.join(tags)
    print(tokens)
    print(tags)
    return tags


def allennlp_tag(message, predictor):
    result = predictor.predict(message)
    tokens = result['tokens']
    tags = result['pos_tags']

    indices = []
    for i in range(len(tokens)):
        s = str(tokens[i])
        if s.startswith('file_name>') or s.startswith('version>') or s.startswith('url>') \
                or s.startswith('enter>') or s.startswith('tab>') or s.startswith('iden>'):
            indices.append(i)
        elif s.endswith('<file_name') or s.endswith('<version') or s.endswith('<url') \
                or s.endswith('<enter') or s.endswith('<tab') or s.endswith('<iden'):
            indices.append(i)

    new_tokens = []
    new_tags = []
    for i in range(len(tokens)):
        if i in indices:
            s = str(tokens[i])
            if s.startswith('file_name>'):
                s = s.replace('file_name>', '')
                new_tokens.append('file_name')
                new_tags.append('XX')
                new_tokens.append('>')
                new_tags.append('XX')
                new_tokens.append(s)
                new_tags.append('XX')
            elif s.startswith('version>'):
                s = s.replace('version>', '')
                new_tokens.append('version')
                new_tags.append('XX')
                new_tokens.append('>')
                new_tags.append('XX')
                new_tokens.append(s)
                new_tags.append('XX')
            elif s.startswith('url>'):
                s = s.replace('url>', '')
                new_tokens.append('url')
                new_tags.append('XX')
                new_tokens.append('>')
                new_tags.append('XX')
                new_tokens.append(s)
                new_tags.append('XX')
            elif s.startswith('enter>'):
                s = s.replace('enter>', '')
                new_tokens.append('enter')
                new_tags.append('XX')
                new_tokens.append('>')
                new_tags.append('XX')
                new_tokens.append(s)
                new_tags.append('XX')
            elif s.startswith('tab>'):
                s = s.replace('tab>', '')
                new_tokens.append('tab')
                new_tags.append('XX')
                new_tokens.append('>')
                new_tags.append('XX')
                new_tokens.append(s)
                new_tags.append('XX')
            elif s.startswith('iden>'):
                s = s.replace('iden>', '')
                new_tokens.append('iden')
                new_tags.append('XX')
                new_tokens.append('>')
                new_tags.append('XX')
                new_tokens.append(s)
                new_tags.append('XX')
            elif s.endswith('<file_name'):
                s = s.replace('<file_name', '')
                new_tokens.append(s)
                new_tags.append('XX')
                new_tokens.append('<')
                new_tags.append('XX')
                new_tokens.append('file_name')
                new_tags.append('XX')
            elif s.endswith('<version'):
                s = s.replace('<version', '')
                new_tokens.append(s)
                new_tags.append('XX')
                new_tokens.append('<')
                new_tags.append('XX')
                new_tokens.append('version')
                new_tags.append('XX')
            elif s.endswith('<url'):
                s = s.replace('<url', '')
                new_tokens.append(s)
                new_tags.append('XX')
                new_tokens.append('<')
                new_tags.append('XX')
                new_tokens.append('url')
                new_tags.append('XX')
            elif s.endswith('<enter'):
                s = s.replace('<enter', '')
                new_tokens.append(s)
                new_tags.append('XX')
                new_tokens.append('<')
                new_tags.append('XX')
                new_tokens.append('enter')
                new_tags.append('XX')
            elif s.endswith('<tab'):
                s = s.replace('<tab', '')
                new_tokens.append(s)
                new_tags.append('XX')
                new_tokens.append('<')
                new_tags.append('XX')
                new_tokens.append('tab')
                new_tags.append('XX')
            elif s.endswith('<iden'):
                s = s.replace('<iden', '')
                new_tokens.append(s)
                new_tags.append('XX')
                new_tokens.append('<')
                new_tags.append('XX')
                new_tokens.append('iden')
                new_tags.append('XX')
        else:
            new_tokens.append(tokens[i])
            new_tags.append(tags[i])
    tokens = new_tokens
    tags = new_tags
    length = len(tokens)

    new_tokens = []
    new_tags = []
    targets = ['file_name', 'version', 'url', 'enter', 'tab', 'iden']
    i = 0
    while i < length:
        if i < length-2 and tokens[i] == '<' and tokens[i+1] in targets and tokens[i+2] == '>':
            new_tokens.append(tokens[i] + tokens[i+1] + tokens[i+2])
            new_tags.append('XX')
            i += 3
        else:
            new_tokens.append(tokens[i])
            new_tags.append(tags[i])
            i += 1

    tokens = new_tokens
    tags = new_tags
    length = len(tokens)
    tokens = ' '.join(tokens)
    tags = ' '.join(tags)
    print('----------------------------------------------------------------------')
    print(tokens)
    print(tags)
    # print(trees)
    return tokens, tags, length


def update_tags(cursor, db, tokens, tags, length, id):
    sql = "update message set allennlp_len = %d, allennlp_tokens = '%s', allennlp_tags = '%s' where id = %d" % \
          (length, tokens.replace("'", "''"), tags.replace("'", "''"), id)
    print(sql)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()


if __name__ == "__main__":
    db = MySQLdb.Connect(host='localhost', port=3306, user='root', passwd='123456', db='commit_message',
                         charset='utf8')
    cursor = db.cursor()
    sql = "select id, repo_id, url, label, new_message1 from message where new_message1 is not null"
    cursor.execute(sql)
    rows = cursor.fetchall()
    # db.close()

    archive = load_archive('../../tool/elmo-constituency-parser-2018.03.14.tar.gz', 0)
    predictor = Predictor.from_archive(archive, 'constituency-parser')

    for row in rows:
        # message = str(row[4]).replace(' <enter> ', ' ').replace(' <tab> ', ' ')
        message = row[4]
        tokens, tags, length = allennlp_tag(message, predictor)
        update_tags(cursor, db, tokens, tags, length, row[0])

    db.close()
