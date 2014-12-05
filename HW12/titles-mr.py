from mrjob.job import MRJob
from combine_user_visits import csv_readline
import string
from natsort import natsorted

list = []

class TopPages(MRJob):

    def mapper(self, line_no, line):
        cell = csv_readline(line)
        if cell[0] == 'A':
            title = cell[3].split(' ')
            for word in title:
                word = word.translate(None, string.punctuation)
                yield (word, 1)

    def reducer(self, word, word_counts):
        tuple = (sum(word_counts), word)
        list.append(tuple)

if __name__ == '__main__':
    TopPages.run()
    for tuple in natsorted(list, reverse=True)[0:10]:
        print tuple