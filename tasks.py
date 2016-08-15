from invoke import task, run, call
import contextlib
import os
import shutil


@task
def mkdocs(clean=False):
    """ Only build site pages using MkDocs.
    :param clean: should site folder be emptied before build?
    """
    # use readme file for rendering index page
    index_path = os.path.join("docs", "index.md")
    index_backup_path = "index.md_original"

    with backed_up_file(index_path, index_backup_path):
        shutil.copy2("README.md", index_path)
        run("mkdocs build" + (" --clean" if clean else ""), echo=True)


@task
def api():
    """ Compiles API reference into site folder. """
    lein("codox")


@task(post=[call(mkdocs, clean=True), call(api)])
def site():
    """ Builds project site (including API docs). """
    pass


################################################### Helpers
@contextlib.contextmanager
def backed_up_file(filepath, backup_path):
    """ File will be returned to its initial state on context exit. """
    with temp_file(backup_path):
        try:
            print("copy " + filepath + " to backup " + backup_path)
            shutil.copy2(filepath, backup_path)
            yield
        finally:
            print("recover " + filepath + " from backup " + backup_path)
            shutil.copy2(backup_path, filepath)


@contextlib.contextmanager
def temp_file(filepath):
    try:
        print("create temp file " + filepath)
        open(filepath, 'w').close()
        yield
    finally:
        print("remove temp file " + filepath)
        os.remove(filepath)


@contextlib.contextmanager
def chdir(dirname):
    curdir = os.getcwd()
    try:
        os.chdir(dirname)
        print("current dir: " + dirname)
        yield
    finally:
        os.chdir(curdir)


def lein(args):
    run("lein {0}".format(args), echo=True)
