from __future__ import annotations

import sys

import streamlit_authenticator as stauth


def main() -> None:
    passwords = sys.argv[1:]
    if not passwords:
        print("Usage: python -m app.ui.generate_password_hashes <password1> <password2> ...")
        return
    hashes = stauth.Hasher(passwords).generate()
    for idx, value in enumerate(hashes, start=1):
        print(f"{idx}: {value}")


if __name__ == "__main__":
    main()
