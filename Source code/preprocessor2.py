import MySQLdb
from urllib.request import urlopen
from urllib.request import Request
import json


def filter_tokens(length, tokens, tags):
    indices = []
    tokens = tokens.split(' ')
    tags = tags.split(' ')
    for i in range(length):
        if str(tags[i]).startswith("NN"):
            # if str(tokens[i]) == 'file_name' or str(tokens[i]) == 'version':
            #     continue
            indices.append(i)
    return indices, tokens


def search_in_patches(url, indices, tokens):
    url = url.replace('https://github.com/', 'https://api.github.com/repos/').replace('/commit/', '/commits/')
    req = Request(url)  # + '?access_token=')
    response = urlopen(req).read()
    commit = json.loads(response.decode())
    files = commit['files']
    patches = []
    for file in files:
        if 'patch' in file.keys():
            patch = file['patch']
            patches.append(patch)

    found_indices = []
    found_tokens = []
    for index in indices:
        for patch in patches:
            if str(patch).find(tokens[index]) > -1:
                found_indices.append(index)
                found_tokens.append(tokens[index])
                break
    return found_indices, found_tokens


def escape(message, replacement):
    start = 0
    escapes = []
    index = str(message).find(replacement, start, len(message))
    while index > -1:
        escapes.append([index, index + len(replacement)])
        start = index + len(replacement)
        index = str(message).find(replacement, start, len(message))
    return escapes


def replace_tokens(message, tokens):
    escs = []
    replacements = ['<file_name>', '<version>', '<url>', '<enter>', '<tab>']
    for replacement in replacements:
        esc = escape(message, replacement)
        escs += esc

    # find out start and end index of replaced tokens
    locations = []
    end = 0
    for t in tokens:
        # start = str(message).index(t, end, len(message))
        in_escape = True
        while in_escape:
            start = str(message).find(t, end, len(message))
            in_escape = False
            for esc in escs:
                if start in range(esc[0], esc[1]):
                    in_escape = True
                    end = esc[1]
                    break
        end = start + len(t)
        locations.append([start, end])

    # merge continuous replaced tokens
    new_locations = []
    i = 0
    start = -1
    while i < len(locations):
        if start < 0:
            start = locations[i][0]
        if i < len(locations) - 1 and locations[i + 1][0] - locations[i][1] < 2:
            i += 1
            continue
        else:
            end = locations[i][1]
            new_locations.append([start, end])
            start = -1
            i += 1

    # replace tokens in message with <file_name>
    end = 0
    new_message = ""
    for location in new_locations:
        start = location[0]
        new_message += message[end:start]
        new_message += "<iden>"
        end = location[1]
    new_message += message[end:len(message)]

    return new_message


def update_found(id, prediction, db, cursor):
    sql = "update message set found = %d where id = %d" % (prediction, id)
    # print(sql)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()


def update_new_message1(id, new_message, db, cursor):
    sql = "update message set new_message1 = '%s' where id = %d" % (new_message.replace("'", "''"), id)
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
    sql = "select id, repo_id, label, prediction, url, new_message, allennlp_len, allennlp_tokens, allennlp_tags " \
          "from message where new_message is not null"
    cursor.execute(sql)
    rows = cursor.fetchall()

    for row in rows:
        id = row[0]
        label = row[2]
        url = row[4]
        new_message = row[5]
        length = row[6]
        tokens = row[7]
        tags = row[8]
        indices, tokens = filter_tokens(length, tokens, tags)
        if len(indices) > 0:
            found_indices, found_tokens = search_in_patches(url, indices, tokens)
            if len(found_indices) > 0:
                new_message = replace_tokens(new_message, found_tokens)
                # update_new_message1(id, new_message, db, cursor)
                # continue
        update_new_message1(id, new_message, db, cursor)

    db.close()
