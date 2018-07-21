from os import listdir
from os.path import join, isfile
from multiprocessing.dummy import Pool
from functools import partial
import time
import zipfile as zf


def get_all_archives_paths(main_path):
    """Function parses list of paths where all archives are located
    in main_path.

    :param main_path: path to directory containing all the archives
    :type main_path: str
    :return: archives_paths (list)
    """
    archives_paths = []

    all_directory_objects = listdir(main_path)

    for directory_object in all_directory_objects:
        directory_object_path = join(main_path, directory_object)

        if isfile(directory_object_path) and ".zip" in directory_object:
            archives_paths.append(directory_object_path)

    return archives_paths


def show_archives_and_number(any_archives_list):
    """Special procedure shows information about list of available archives
    (in any_archives_list).

    :param any_archives_list: sequence containing paths to archives
    :type any_archives_list: list
    :return: None
    """
    number_of_archives = len(any_archives_list)
    print(
        "Number of found archives: {n}".format(n=number_of_archives),
        "Paths:",
        sep="\n"
    )
    if number_of_archives > 10:
        print(
            "\n".join(
                map(lambda string: "-> " + string, any_archives_list[0:3])
            ) + "\n...\n" + "\n".join(
                map(lambda string: "-> " + string, any_archives_list[-4:-1])
            )
        )
    else:
        print(
            "\n".join(map(lambda string: "-> " + string, any_archives_list))
        )


def get_search_query():
    """Function gets user's query for searching data.
    Value is entered by the keyboard.

    :return: processed_search_string (set)
    """
    processed_search_string = set(
        input("Enter your query here: ").lower().split()
    )

    return processed_search_string


def show_time(start_time):
    """Extra functions shows program running time.

    :param start_time: time when the algorithm was started
    :type start_time: float
    :return: None
    """
    time_sec = time.time() - start_time
    time_min = int(time_sec) // 60
    time_sec = time_sec - time_min * 60

    print("Done in {} m {:.3f} s.".format(
        time_min,
        time_sec
    ))


def work_with_all_archives(archives_paths, processes_number, any_query,
                           time_start):
    """Procedure iterates list with paths of archives (archives_paths).

    :param archives_paths: sequence containing paths to archives
    :type archives_paths: list
    :param processes_number: number of using processes in multiprocessing
    :type processes_number: int
    :param any_query: user's query to search data
    :type any_query: set
    :param time_start: time is checked when algorithm began working
    :type time_start: float
    :return: None
    """
    p = Pool(processes_number)
    p.map(
        partial(
            work_with_current_archive,
            given_query=any_query
        ),
        archives_paths
    )

    show_time(time_start)


def work_with_current_archive(current_archive_path, given_query):
    """Procedure works with iteration archive is located in archive
    (current_archive_path).

    :param current_archive_path: string containing archive path of
        current iteration
    :type current_archive_path: str
    :param given_query: user's query to search data
    :type given_query: set
    :return: None
    """
    with zf.ZipFile(current_archive_path, "r", compression=zf.ZIP_DEFLATED) \
            as zip_file_object:
        archive_files_names = zip_file_object.namelist()

        work_with_all_files(
            archive_files_names,
            zip_file_object,
            given_query
        )


def work_with_all_files(files_names, zip_object, any_query):
    """Procedure iterates list of files are contained in zip file.

    :param files_names: sequence containing names of files in archive
    :type files_names: list
    :param zip_object: object to work with archive files
    :type zip_object: zf.ZipFile
    :param any_query: user's query to search data
    :type any_query: set
    :return: None
    """

    for file_name in files_names:
        work_with_current_file(file_name, zip_object, any_query)


def work_with_current_file(name_of_file, zip_file_object, any_query):
    """Procedure works with current file is contained in zip archive.
    Searches strings in file data, decodes it and matches it with user's
    query.

    :param name_of_file: string containing name of text file in archive
    :type name_of_file: str
    :param zip_file_object: object to work with archive files
    :type zip_file_object: zf.ZipFile
    :param any_query: user's query
    :type any_query: set
    :return: None
    """
    with zip_file_object.open(name_of_file, force_zip64=True) as file_object:
        while True:
            data_string = file_object.readline()
            if not data_string:
                break
            else:
                data_string_decoded = data_string.decode("1251")
                if any_query < set(data_string_decoded.lower().split()):

                    print(data_string_decoded.replace("\n", ""))
                    # Data string we need.


if __name__ == "__main__":
    START: float = time.time()
    N_PROCESSES: int = 20
    CURRENT_PATH: str = r""
    # Enter path of archives here (CURRENT_PATH).
    # Example: C:\path\to\your\files

    archives_paths_list = get_all_archives_paths(CURRENT_PATH)
    show_archives_and_number(archives_paths_list)

    text_query = get_search_query()

    work_with_all_archives(
        archives_paths_list,
        N_PROCESSES,
        text_query,
        START
    )
