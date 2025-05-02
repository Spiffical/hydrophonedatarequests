# hydro_dl/ui.py
import sys
from typing import List, Union

from hydrophone.utils.exceptions import UserAbortError

def prompt_pick(opts: List[str], title: str, allow_multiple: bool = False) -> Union[int, List[int]]:
    """Prompts the user to pick option(s) from a numbered list.
    
    Args:
        opts: List of options to choose from
        title: Title to display above the options
        allow_multiple: If True, allows selecting multiple options using comma/space separated numbers
        
    Returns:
        If allow_multiple=False: A single integer index
        If allow_multiple=True: A list of integer indices
    """
    print(f"\n== {title} ==")
    if not opts:
        print("No options available.")
        raise ValueError("Cannot prompt with empty options list.")

    for i, o in enumerate(opts):
        print(f"[{i}] {o}")

    while True:
        try:
            if allow_multiple:
                choice_str = input(f"Enter number(s) (0 to {len(opts)-1}, separate multiple with comma/space): ")
            else:
                choice_str = input(f"Enter number (0 to {len(opts)-1}): ")
                
            if not choice_str: # Handle empty input
                print("✖ Please enter number(s).")
                continue

            if allow_multiple:
                # Split on both commas and spaces
                choices = []
                for part in choice_str.replace(',', ' ').split():
                    try:
                        x = int(part)
                        if 0 <= x < len(opts):
                            choices.append(x)
                        else:
                            print(f"✖ Number {x} out of range (0-{len(opts)-1}).")
                            choices = []
                            break
                    except ValueError:
                        print(f"✖ Invalid number: {part}")
                        choices = []
                        break
                
                if choices:
                    # Remove duplicates while preserving order
                    seen = set()
                    choices = [x for x in choices if not (x in seen or seen.add(x))]
                    return choices
            else:
                x = int(choice_str)
                if 0 <= x < len(opts):
                    return x
                else:
                    print(f"✖ Number out of range (0-{len(opts)-1}).")
        except ValueError:
            if allow_multiple:
                print("✖ Invalid input. Please enter numbers separated by commas or spaces.")
            else:
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