import random
from hangman_art import hangman_art

def load_words(filename):
    """Load words from file and return as list"""
    with open(filename, 'r') as file:
        #Load words from file and return as list
        flat_list = [word for line in file for word in line.split(',')]
    return flat_list

def choose_random_word(word_list):
    """Select random word from list"""
    return random.choice(word_list)

def display_hangman(wrong_count):
    """Display ASCII hangman based on wrong guess count"""
    if wrong_count >= 0 and wrong_count < len(hangman_art):
        print(hangman_art[wrong_count])
    else:
        print("Invalid wrong count.")


def display_game_state(word, guessed_letters, wrong_letters, wrong_count):
    """Display current game state to user"""
    print(f"Word: {' '.join(letter if letter in guessed_letters else '_' for letter in word)}")
    print(f"Wrong letters: {', '.join(wrong_letters)}")
    print(f"Attempts remaining: {len(hangman_art) - wrong_count}")

def get_player_guess(guessed_letters):
    """Get and validate player's letter guess"""
    while True:
        guess = input("Enter a letter: ").lower()
        if len(guess) == 1 and guess.isalpha() and guess not in guessed_letters:
            return guess
        else:
            print("Invalid input. Please enter a single character that you haven't guessed yet.")

def check_game_over(word, guessed_letters, wrong_count, max_wrong):
    """Check if game is won or lost"""
    if all(letter in guessed_letters for letter in word):
        print("Congratulations! You've won!")
        return True
    elif wrong_count >= max_wrong:
        print("Game over! You've lost!")
        return True
    return False


def play_hangman():
    """Main game function"""
    filename = 'words.txt'
    word_list = load_words(filename)
    word = choose_random_word(word_list)
    guess_letters = set()
    wrong_letters = set()
    wrong_count = 0
    max_wrong = len(hangman_art) - 1
    print("=== HANGMAN GAME ===")
    while True:
        display_hangman(wrong_count)
        display_game_state(word, guess_letters, wrong_letters, wrong_count)
        guess = get_player_guess(guess_letters)

        if guess in word:
            guess_letters.add(guess)
            print("Good guess!")
        else:
            wrong_letters.add(guess)
            wrong_count += 1
            print("Wrong guess!")
            
        if check_game_over(word, guess_letters, wrong_count, max_wrong):
            display_hangman(wrong_count)
            print(f"The word was: {word}")
            break
        
def main():
    """Main program entry point"""
    try:
        play_hangman()
    except FileNotFoundError:
        print("Error: The words file {filename} was not found. Please ensure it exists in the same directory as this script.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()