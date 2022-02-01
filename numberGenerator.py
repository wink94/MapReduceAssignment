import random
outfile = open('input.txt', 'a+')

for rows in range(10):

    cols = random.randint(1, 5)

    for count in range(cols):
        num = random.randint(1, 100)

        if count == cols-1:
            outfile.write(str(num)+"\n")
        else:
            outfile.write(str(num)+",")
    # print('\n')