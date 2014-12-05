"""Find Vroots with more than 400 visits.

This program will take a CSV data file and output tab-seperated lines of

    Vroot -> number of visits

To run:

    python top_pages.py anonymous-msweb.data

To store output:

    python top_pages.py anonymous-msweb.data > top_pages.out
"""

from mrjob.job import MRJob
from combine_user_visits import csv_readline
import string

class TopPages(MRJob):

    def mapper(self, line_no, line):
        """Extracts the Vroot that was visited"""
        cell = csv_readline(line)
        if cell[0] == 'V':
            yield (cell[3], 1)

    def reducer(self, user, visit_counts):
        totalVisits = sum(visit_counts)
        if totalVisits > 19:
            yield (totalVisits, user)

if __name__ == '__main__':
    TopPages.run()
