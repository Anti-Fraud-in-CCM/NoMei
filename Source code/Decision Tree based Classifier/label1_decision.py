import MySQLdb
import random
import collections
import nltk
from nltk.stem.snowball import SnowballStemmer
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier


db = MySQLdb.Connect(host='localhost', port=3306, user='root', passwd='123456', db='commit_message',
                     charset='utf8')
cursor = db.cursor()


def get_data():
    sql = "select id, repo_id, n_label, new_message1, allennlp_tokens, allennlp_tags" \
          " from message where new_message1 is not null and id > 3045 and id < 5678"
    cursor.execute(sql)
    rows = cursor.fetchall()

    repo_id = 101
    data_set = []
    repo = []
    for row in rows:
        if row[1] != repo_id:
            data_set.append(repo)
            repo = []
            repo_id = row[1]
        if row[2] is not None and int(row[2]) == 1:
            label = 1
        else:
            label = 0
        repo.append([row[0], row[1], label, row[3], row[4], row[5]])
    data_set.append(repo)

    for repo in data_set:
        repo_id = repo[0][1]
        if repo_id < 101:
            repo[0].append(1)
            for i in range(1, len(repo)):
                repo[i].append(0)
        else:
            for i in range(len(repo)-1):
                repo[i].append(0)
            repo[len(repo)-1].append(1)
    return data_set


def build_vocab(data_set):
    data = []
    for repo in data_set:
        for item in repo:
            data += item[2]
    counter = collections.Counter(data)
    count_pairs = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    words, _ = list(zip(*count_pairs))
    word_to_id = dict(zip(words, range(len(words))))
    word_to_id['<pad>'] = len(words)
    return word_to_id


def convert_to_id(data, word_to_id):
    new_data = []
    for item in data:
        tokens = item[2]
        tokens = [word_to_id[token] for token in tokens]
        new_data.append([item[0], item[1], tokens, item[3]])
    return new_data


def tokenize(data):
    stemmer = SnowballStemmer('english')
    new_data = []
    for item in data:
        id = item[0]
        label = item[2]
        tokens = str(item[4]).lower().replace(' <enter>', '').replace(' <tab>', '').split(' ')
        is_first_commit = item[6]

        new_tokens = []
        for token in tokens:
            for s in token:
                if str(s).isalnum():
                    new_tokens.append(token)
                    break
        tokens = new_tokens

        adjective = ['initial', 'primer', 'first', 'second', 'final', 'all', 'temporary', '1st', '2nd', 'for', 'correct']
        noun = ['commit', 'push', 'upload', 'import', 'commits', 'imports', 'submit', 'submission', 'commiting']
        new_tokens = []
        i = 0
        while i < len(tokens)-1:
            if (tokens[i+1] in adjective and tokens[i] in noun) or (tokens[i] in adjective and tokens[i+1] in noun):
                if i == 0:
                    new_tokens.append('<start>')
                new_tokens.append(tokens[i])
                new_tokens.append(tokens[i+1])
                if i == len(tokens) - 2:
                    new_tokens.append('<end>')
                i += 1
            i += 1
        tokens = new_tokens

        tokens = [stemmer.stem(token) for token in tokens]
        new_data.append([id, label, tokens, is_first_commit])
    return new_data


def process_testset(test_set):
    set = []
    for repo in test_set:
        for item in repo:
            sample = [x for x in item[2]]
            while len(sample) < 4:
                sample.append(word_to_id['<pad>'])
                # sample.append('<pad>')
            set.append([item[0], item[1], sample, item[3]])
    return set


def process_trainset(train_set):
    pos = []
    neg = []
    for repo in train_set:
        for item in repo:
            sample = [x for x in item[2]]
            while len(sample) < 4:
                sample.append(word_to_id['<pad>'])
                # sample.append('<pad>')
            if item[1] == 1:
                if len(item[2]) > 0:
                    pos.append([item[0], item[1], sample, item[3]])
            elif item[1] == 0:
                neg.append([item[0], item[1], sample, item[3]])
    times = int(len(neg)/len(pos))
    for i in range(times):
        neg += pos
    random.shuffle(neg)
    return neg


def update_isFirst(id, prediction):
    sql = "update message set isFirst_decision = %d where id = %d" % (prediction, id)
    # print(sql)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()


def train_svm(train_set):
    tokens_list = []
    label_list = []
    for item in train_set:
        label_list.append(item[1])
        tokens_list.append(item[2] + [item[3]])
    # model = SVC()
    model = DecisionTreeClassifier()
    model.fit(tokens_list, label_list)
    return model


def evaluate(model, test_set):
    tp = 0
    tn = 0
    fp = 0
    fn = 0
    for sample in test_set:
        id = sample[0]
        label = sample[1]
        tokens = sample[2]
        is_first_commit = sample[3]

        prediction = model.predict([tokens + [is_first_commit]])
        update_isFirst(id, prediction)
        if label == 1:
            if prediction == 1:
                tp += 1
            else:
                fn += 1
        else:
            if prediction == 1:
                fp += 1
            else:
                tn += 1
    if tp+fp == 0:
        precision = 0
    else:
        precision = tp * 1.0 / (tp + fp)
    if tp+fn == 0:
        recall = 0
    else:
        recall = tp * 1.0 / (tp + fn)
    if precision+recall == 0:
        f1 = 0
    else:
        f1 = 2 * precision * recall / (precision + recall)
    print('total =', tp + tn + fp + fn)
    print('precision =', precision)
    print('recall =', recall)
    print('f1 =', f1)
    return tp, fn, fp, tn


if __name__ == "__main__":
    data_set = get_data()
    data_set = [tokenize(repo) for repo in data_set]
    word_to_id = build_vocab(data_set)
    data_set = [convert_to_id(repo, word_to_id) for repo in data_set]
    batch_size = 25
    batch_num = int(len(data_set)/batch_size)
    tps = 0
    fns = 0
    fps = 0
    tns = 0
    for i in range(batch_num):
        print('-----------------------------------------------')
        print('iteration', i)
        test_set = data_set[batch_size * i: batch_size * (i + 1)]
        test_set = process_testset(test_set)

        train_set = data_set[:batch_size * i]
        train_set += data_set[batch_size * (i + 1):]
        train_set = process_trainset(train_set)

        model = train_svm(train_set)
        tp, fn, fp, tn = evaluate(model, test_set)
        tps += tp
        fns += fn
        fps += fp
        tns += tn

    precision = tps * 1.0 / (tps + fps)
    recall = tps * 1.0 / (tps + fns)
    f1 = 2 * precision * recall / (precision + recall)
    print('---------------------------------------------------')
    print('total =', tps + tns + fps + fns)
    print('precision =', precision)
    print('recall =', recall)
    print('f1 =', f1)
    print('tp =', tps)
    print('tn =', tns)
    print('fn =', fns)
    print('fp =', fps)

    db.close()