#[macro_use]
extern crate clap;
extern crate walkdir;
extern crate blake2;
extern crate byteorder;
extern crate unbytify;

use std::path::PathBuf;

use std::collections::{HashMap, HashSet};
use blake2::{Blake2b, Digest};
use walkdir::{WalkDir, DirEntry};

use std::ops::Deref;

use std::collections::hash_map::Entry::Occupied;

use std::rc::Rc;
use std::fmt;
use std::fs;
use std::ffi::OsStr;

use std::os::unix::ffi::OsStrExt;

use byteorder::{LittleEndian, WriteBytesExt};


#[derive(PartialEq ,Eq, Hash, Clone)]
struct FileHash(Vec<u8>);

#[derive(Debug)]
struct DirectoryData {
    path: PathBuf,
    children_hashes: Vec<FileHash>,
    descendant_number: u64,
    disk_size: u64,
}

impl fmt::Debug for FileHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let &FileHash(ref slice) = self;
        write!(f, "FileHash {:?}", &slice[..5])
    }
}

impl Deref for FileHash {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl DirectoryData {
    fn hash(&self) -> FileHash {
        let mut digest = Blake2b::default();
        for hash in &self.children_hashes {
            digest.input(&hash)
        }
        FileHash(digest.result().to_vec())
    }
}

fn crawl_directory(root: PathBuf, map: &mut HashMap<FileHash, Vec<Rc<DirectoryData>>>, inodes : &mut HashSet<u64>) -> Rc<DirectoryData> {
    let mut subdir_paths = Vec::new();
    let mut files_paths = Vec::new();
    for dir_entry_result in WalkDir::new(&root).follow_links(false).max_depth(1) {
        match dir_entry_result {
            Ok(dir_entry) => {
                let path = dir_entry.path().to_path_buf();
                if path == root { // Walkdir liste aussi la racine, il faut donc l'enlever pour éviter les récursions infinies
                    continue;
                }
                if dir_entry.file_type().is_file() {
                    files_paths.push(path);
                } else if dir_entry.file_type().is_dir() {
                    if check_inode(inodes, &dir_entry) {
                        subdir_paths.push(path);
                    }
                }
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }

    let mut dir_data = DirectoryData {
        path: root,
        children_hashes: Vec::new(),
        descendant_number: files_paths.len() as u64,
        disk_size: 0,
    };

    for dir_path in subdir_paths {
        let subdir_data = crawl_directory(dir_path, map, inodes);
        let hash = subdir_data.hash();
        dir_data.children_hashes.push(hash);
        dir_data.descendant_number += 1 + subdir_data.descendant_number;
        dir_data.disk_size += subdir_data.disk_size;
    }

    for file_path in files_paths {
        let hash = hash_file_metadata(&file_path);
        dir_data.children_hashes.push(hash);
        let size = fs::metadata(&file_path).expect(&format!("impossible to access metadata at path: {:?}", file_path)).len();
        dir_data.disk_size += size;
    }

    let rc_dir_data = Rc::new(dir_data);
    let map_entry = map.entry(rc_dir_data.hash()).or_insert(Vec::new());

    map_entry.push(rc_dir_data.clone());
    rc_dir_data
}

fn list_duplicates(map: HashMap<FileHash, Vec<Rc<DirectoryData>>>, min_size :u64) -> Vec<Vec<Rc<DirectoryData>>> {
    let mut result = vec![];

    let mut already_found_hashes : HashMap<FileHash, usize> = HashMap::new();

    let mut vect_of_key_and_entries  = map.into_iter().collect::<Vec<_>>();

    vect_of_key_and_entries.sort_unstable_by(|a, b| {
        let first_element_of_a = a.1.get(0).expect("empty vector that should not be empty at line 119"); // the vectors are never empty
        let first_element_of_b = b.1.get(0).expect("empty vector that should not be empty at line 120"); // the vectors are never empty
        first_element_of_b.descendant_number.cmp(&first_element_of_a.descendant_number)
    });

    for entry in vect_of_key_and_entries {
        let (key, value) = entry;
        // ignore les répertoires en un seul exemplaire
        if value.len() == 1 {
            continue;
        }

        let mut add_to_result = true;
        // pour ceux dont le parent a déjà été marqué, on rajoute leurs enfants dans le set des entrées déjà traités
        if let Occupied(entry) = already_found_hashes.entry(key) {
            // si le dossier a été trouvé exactement autant de fois que dans les parents déjà traités, on ignore le dossier et on se contente juste de marquer les enfants comme étant déjà traités
            if &(value.len()) == entry.get() {
                add_to_result = false;
            }
        }

        // pour ceux qui sont en plusieurs exemplaires et qui n'apparaissent pas dans le set,
        // ou bien apparaissent dans le set en quantité inférieure à leur nombre d'occurence, ce qui veut dire qu'il existe un doublons en dehors des dossiers déjà traités
        // on les ajoute à la liste des résultats, et on met tous leurs enfants dans le set
        {
            let first_dir_data = value.get(0).expect("empty vector that should not be empty at line 144");
            for children_hash in &(first_dir_data.children_hashes) {
                let mut entry = already_found_hashes.entry(children_hash.clone());
                let num = entry.or_insert(0);
                *num += value.len();
            }
            if first_dir_data.descendant_number == 0 || first_dir_data.disk_size < min_size{
                add_to_result = false;
            }
        }

        if add_to_result {
            result.push(value);
        }

    }

    result
}

fn hash_file_metadata(path: &PathBuf) -> FileHash {
    //hash file name
    let file_name = path.file_name();
    let mut digest = Blake2b::default();
    digest.input(file_name.unwrap_or_else(||{
        eprintln!("No filename at path: {:?}",path );
        OsStr::new("no file name")
    }).as_bytes());

    let size = fs::metadata(path).expect(&format!("impossible to access metadata at path: {:?}", path)).len();
    let mut wtr = vec![];
    wtr.write_u64::<LittleEndian>(size).expect(&format!("failed to transform size {:?} to &[u8] at path: {:?}", size, path));
    digest.input(&wtr);

    FileHash(digest.result().to_vec())
}

#[cfg(unix)]
fn check_inode(set: &mut HashSet<u64>, entry: &DirEntry) -> bool {
    set.insert(entry.ino())
}
#[cfg(not(unix))]
fn check_inode(_: &mut HashSet<u64>, _: &DirEntry) -> bool {
    true
}

fn validate_byte_size(s: String) -> Result<(), String> {
    unbytify::unbytify(&s).map(|_| ()).map_err(
        |_| format!("{:?} is not a byte size", s))
}

fn main() {
    let args = clap_app!(dupdirfinder =>
        (version: crate_version!())
        (author: "Kevin Canévet, 2017")
        (about: "A duplicate directory finder.")
        (@arg minsize: -m [MINSIZE] default_value("1") validator(validate_byte_size)
         "Minimum file size to consider")
        (@arg root: +required +multiple "Root directory or directories to search.")
    ).get_matches();

    let roots = args.values_of("root").unwrap();
    let minsize = unbytify::unbytify(args.value_of("minsize").unwrap()).unwrap();

    // We take care to avoid visiting a single inode twice,
    // which takes care of (false positive) hardlinks.
    let mut inodes = HashSet::default();


    for root in roots {
        println!("Checking {} directory", root);
        println!("");

        let mut map = HashMap::new();
        let root = PathBuf::from(root);
        crawl_directory(root, &mut map, &mut inodes);
        let duplicates = list_duplicates(map, minsize);

        for duplicate in duplicates {
            println!("Duplicat de {:?} répertoires", duplicate.len());
            let duplicate_size = duplicate.get(0).expect("yet another expectation not met").disk_size;
            let space_wasted = (duplicate.len() - 1 ) as u64 * duplicate_size;
            let (val, suffix) = unbytify::bytify(space_wasted);
            println!("    Space wasted {:.1} {}", val, suffix);
            for dir in duplicate {
                println!("{:?}", dir.path);
            }

            println!("");
        }
        println!("");
    }
}
