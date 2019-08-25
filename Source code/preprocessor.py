import re
import MySQLdb


db = MySQLdb.Connect(host='localhost', port=3306, user='root', passwd='123456', db='commit_message',
                     charset='utf8')
cursor = db.cursor()


def select_message():
    sql = "select id, repo_id, url, label, message, first_file_id, file_num from message " \
          "where repo_id > 100 and label is not null"
          # "where url is not null"
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows


def select_file(message_id):
    sql = "select file_name from file where message_id = " + str(message_id)
    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows


def update_new_message(id, new_message):
    sql = "update message set new_message = '%s' where id = %d" % (new_message.replace("'", "''"), id)
    print(sql)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()


def split(sentence):
    new_sentence = ''
    for s in sentence:
        if not str(s).isalnum():
            if len(new_sentence) > 0 and not new_sentence.endswith(' '):
                new_sentence += ' '
            if s != ' ':
                new_sentence += s
                new_sentence += ' '
        else:
            new_sentence += s
    tokens = new_sentence.replace('< url >', '<url>').replace('< enter >', '<enter>').replace('< version >', '<version>').strip().split(' ')
    return tokens


def find_url(message):
    pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    urls = re.findall(pattern, message)
    for url in urls:
        message = message.replace(url, '<url>')
    return message


def find_version(message):
    pattern = re.compile(r'\d+(?:\.\d+)+')
    versions = pattern.findall(message)
    for version in versions:
        message = message.replace(version, '<version>')
    return message


def get_all_data():
    samples = []
    rows = select_message()
    for row in rows:
        id = row[0]
        repo_id = row[1]
        url = row[2]
        label = row[3]
        message = row[4]
        first_file_id = row[5]
        file_num = row[6]

        if label == 1 or label == 0:
            files = select_file(id)
            files = [split(file[0]) for file in files]
            message = find_url(message)
            message = find_version(message)
            samples.append([id, message, files])
    print(len(samples))
    return samples


def tokenize(identifier):
    new_identifier = ""
    identifier = list(identifier)
    new_identifier += identifier[0]
    for i in range(1, len(identifier)):
        if str(identifier[i]).isupper() and (str(identifier[i-1]).islower() or (i < len(identifier)-1 and str(identifier[i+1]).islower())):
            if not new_identifier.endswith(" "):
                new_identifier += " "
        new_identifier += identifier[i]
        if str(identifier[i]).isdigit() and i < len(identifier)-1 and not str(identifier[i+1]).isdigit():
            if not new_identifier.endswith(" "):
                new_identifier += " "
    return new_identifier.split(" ")


def replace_file_name(sample):
    # tokenize file paths
    file_names = []
    for file in sample[2]:
        for f in file:
            tokens = tokenize(f)
            for token in tokens:
                file_names.append(token.lower())
            if len(tokens) > 1:
                file_names.append(f.lower())
    # split message
    message = sample[1]
    tokens = split(message)

    replaced_tokens = []
    i = -1
    for t in tokens:
        i += 1
        if t.lower() in file_names and t.isalnum():
            replaced_tokens.append(i)
        else:
            splited_tokens = tokenize(t)
            if len(splited_tokens) > 1:
                found = True
                for s in splited_tokens:
                    if not s.lower() in file_names:
                        found = False
                        break
                if found:
                    replaced_tokens.append(i)

    # deal with other tokens than digits or letters
    new_replaced_tokens = []
    for i in replaced_tokens:
        new_replaced_tokens.append(i)
        if i < len(tokens)-2 and i+2 in replaced_tokens and not tokens[i+1].isalnum():
            new_replaced_tokens.append(i+1)
    replaced_tokens = new_replaced_tokens
    new_replaced_tokens = []
    for i in replaced_tokens:
        if i > 0 and tokens[i-1] == '.' and i-1 not in new_replaced_tokens:
            new_replaced_tokens.append(i-1)
        new_replaced_tokens.append(i)
    replaced_tokens = []
    for i in new_replaced_tokens:
        replaced_tokens.append(tokens[i])

    # find out start and end index of replaced tokens
    locations = []
    end = 0
    for t in replaced_tokens:
        start = str(message).index(t, end, len(message))
        end = start + len(t)
        locations.append([start, end])

    # merge continuous replaced tokens
    new_locations = []
    i = 0
    start = -1
    while i < len(locations):
        if start < 0:
            start = locations[i][0]
        if i < len(locations)-1 and locations[i+1][0] - locations[i][1] < 2:
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
        new_message += "<file_name>"
        end = location[1]
    new_message += message[end:len(message)]

    return new_message


if __name__ == '__main__':
    samples = get_all_data()
    for sample in samples:
        new_message = replace_file_name(sample)
        update_new_message(sample[0], new_message)
    db.close()
