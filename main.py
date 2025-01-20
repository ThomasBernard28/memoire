import os
import questionary
from prompt_toolkit.shortcuts import confirm

mainMenu = [
    {
        'type': 'list',
        'name': 'Main Menu',
        'message': '-------- Welcome to Gitea API Interrogator --------',
        'choices': [
            'Authenticate',
            'Perform Request',
            'Exit'
        ],
        'use_arrow_keys': True
    }
]

confirmExit = [
    {
        'type': 'confirm',
        'name': 'Exit',
        'message': 'Are you sure you want to exit ?',
        'default': False
    }
]

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    answer1 = questionary.prompt(mainMenu)
    if answer1['Main Menu'] == 'Authenticate':
        print('Authenticate')

    elif answer1['Main Menu'] == 'Perform Request':
        print('Perform Request')

    elif answer1['Main Menu'] == 'Exit':
        return


if __name__ == "__main__":
    import sys
    os.system('cls' if os.name == 'nt' else 'clear')

    a = 1
    while a != 0:
        a = main()
        if a != 0:
            answer = questionary.prompt(confirmExit)
            if answer.get('Exit'):
                print('Goodbye!')
                a = 0