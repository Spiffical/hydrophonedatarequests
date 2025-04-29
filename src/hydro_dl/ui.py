# hydro_dl/ui.py
import sys
from typing import List

from .exceptions import UserAbortError

def prompt_pick(opts: List[str], title: str) -> int:
    """Prompts the user to pick an option from a numbered list."""
    print(f"\n== {title} ==")
    if not opts:
        print("No options available.")
        raise ValueError("Cannot prompt with empty options list.") # Or return specific code

    for i, o in enumerate(opts):
        print(f"[{i}] {o}")

    while True:
        try:
            choice_str = input(f"Enter number (0 to {len(opts)-1}): ")
            if not choice_str: # Handle empty input
                print("✖ Please enter a number.")
                continue
            x = int(choice_str)
            if 0 <= x < len(opts):
                return x
            else:
                print(f"✖ Number out of range (0-{len(opts)-1}).")
        except ValueError:
            print("✖ Invalid input, number please.")
        except EOFError:
            print("\n✖ Input stream closed. Aborting.")
            raise UserAbortError("User aborted via EOF.")
        except KeyboardInterrupt:
             print("\n✖ User interruption. Aborting.")
             raise UserAbortError("User aborted via KeyboardInterrupt.")

def confirm_proceed(prompt_message: str = "Proceed?") -> bool:
    """Asks the user for simple yes/no confirmation."""
    while True:
        try:
            proceed = input(f"{prompt_message} [y/N] ").strip().lower()
            if proceed == 'y':
                return True
            elif proceed == 'n' or proceed == '':
                return False
            else:
                print("✖ Please enter 'y' or 'n'.")
        except EOFError:
             print("\n✖ Input stream closed. Aborting.")
             raise UserAbortError("User aborted via EOF.")
        except KeyboardInterrupt:
             print("\n✖ User interruption. Aborting.")
             raise UserAbortError("User aborted via KeyboardInterrupt.")