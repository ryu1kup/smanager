import argparse

from smanager import smanager

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', type=str, default='')
    args = parser.parse_args()
    return args

def show(user):
    manager = smanager(user=user)
    manager.show()

if __name__ == '__main__':
    args = parse_arguments()
    show(args.user)
