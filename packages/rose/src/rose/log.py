from icecream import ic


def write_errors(errs, errs_txt="errors.txt"):
    with open(errs_txt, "w") as f:
        for err in errs:
            f.write(err)
    ic(f"Check {errs_txt} for more information")
