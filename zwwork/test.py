from logs import loger


@loger.logging_read
def test1():
    a = 1 / 0
    print(a)


@loger.logging_read
def test2():
    print('test2')


if __name__ == '__main__':
    test1()
    test2()
