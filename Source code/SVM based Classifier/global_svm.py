import MySQLdb


db = MySQLdb.Connect(host='localhost', port=3306, user='root', passwd='123456', db='commit_message',
                     charset='utf8')
cursor = db.cursor()


def get_data():
    sql = "select id, repo_id, label, isFirst, isSecond, isThird, isForth" \
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
        repo.append([row[0], row[2], row[3], row[4], row[5], row[6]])
    data_set.append(repo)
    return data_set


def update_isBad(id, prediction):
    sql = "update message set isBad = %d where id = %d" % (prediction, id)
    # print(sql)
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()


def evaluate(data_set):
    tp = 0
    tn = 0
    fp = 0
    fn = 0
    for sample in data_set:
        id = sample[0]
        label = sample[1]
        isFirst = int(sample[2])
        isSecond = int(sample[3])
        isThird = int(sample[4])
        isForth = int(sample[5])

        if isFirst + isSecond + isThird + isForth == 0:
            prediction = 0
        else:
            prediction = 1

        update_isBad(id, prediction)
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

    precision = tp * 1.0 / (tp + fp)
    recall = tp * 1.0 / (tp + fn)
    f1 = 2 * precision * recall / (precision + recall)
    print('total =', tp + tn + fp + fn)
    print('precision =', precision)
    print('recall =', recall)
    print('f1 =', f1)
    return tp, fn, fp, tn


if __name__ == "__main__":
    data_set = get_data()

    batch_size = 25
    batch_num = int(len(data_set) / batch_size)
    tps = 0
    fns = 0
    fps = 0
    tns = 0
    for i in range(batch_num):
        print('-----------------------------------------------')
        print('iteration', i)
        test_set = data_set[batch_size * i: batch_size * (i + 1)]
        set = []
        for repo in test_set:
            set += repo
        test_set = set

        tp, fn, fp, tn = evaluate(test_set)
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