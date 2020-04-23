use std::env;
use rocksdb::{DB, ColumnFamilyDescriptor, Options, WriteBatch, IteratorMode};
use std::time::{SystemTime, UNIX_EPOCH};
use rand::{Rng, RngCore};
use std::path::Path;

fn main() {
    let arg: Vec<String> = env::args().collect();
    if arg.len() != 2 {
        println!("Must pass a single arg");
    } else {
        match arg[1].as_str() {
            "rocksdb" => rocksdb(),
            "sled" => sled(),
            _ => println!("Invalid entry.")
        }
    }
}

fn sled() {
    let mut rng = rand::thread_rng();
    let t = sled::open("my_db").unwrap();

    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();


    // insert and get
    for n in 0..1000000 {
        t.insert(rng.next_u32().to_be_bytes(), b"test");
    }
    t.flush();

    let writeEnd = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

    for n in 0..1000000 {
        t.get(rng.next_u32().to_be_bytes());
    }

    // Iterates over key-value pairs, starting at the given key.
    // let scan_key: &[u8] = b"a non-present key before yo!";
    // let mut iter = t.range(scan_key..);
    // assert_eq!(&iter.next().unwrap().unwrap().0, b"yo!");
    // assert_eq!(iter.next(), None);
    //
    // t.remove(b"yo!");
    // assert_eq!(t.get(b"yo!"), Ok(None));

    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

    println!("total - {}", t.iter().count());

    println!("write - {}", (writeEnd - start));
    println!("read - {}", ((end - start) - (writeEnd - start)));
    println!("total - {}", (end - start));
}

fn rocksdb() {
    println!("Running rocksdb...");
    let mut rng = rand::thread_rng();
    let path = "/home/alex/tmp/";
    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);
    let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);

    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    {
        let db = DB::open_cf_descriptors(&db_opts, path, vec![cf]).unwrap();
        let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

        let mut batch = WriteBatch::default();
        for n in 1..1000000 {
            batch.put(rng.next_u32().to_be_bytes(), rng.next_u32().to_be_bytes());
        }
        db.write(batch);
        let writeEnd = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

        //println!("!!!{}", db.iterator(IteratorMode::Start).count());

        let snapshot = db.snapshot();

        for n in 1..1000000 {
            let x = snapshot.get(rng.next_u32().to_be_bytes());
            if x.is_err() {
                println!("{}", x.is_err())
            }
        }

        let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

        println!("write - {}", (writeEnd - start));
        println!("read - {}", ((end - start) - (writeEnd - start)));
        println!("total - {}", (end - start));
    }
    //    let _ = DB::destroy(&db_opts, path);
}
