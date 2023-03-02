#![feature(iter_array_chunks)]
#![feature(slice_flatten)]

use std::{
    fs::File,
    io::read_to_string,
    time::{Duration, Instant},
};

use regex::Regex;
use rusqlite::{Connection, Map, Statement};

use rand::Rng;

fn main() {
    let mut search = String::new();
    let stdin = std::io::stdin();

    println!("Connecting to the database");
    let connection = Connection::open("ips.ldb").unwrap();

    println!("reload database?");
    stdin.read_line(&mut search).expect("Something went wrong!");
    if search.to_lowercase().contains("y") {
        init_data(&connection);
    }

    search.clear();
    println!("enter sql shell?");
    stdin.read_line(&mut search).expect("Something went wrong!");
    if search.to_lowercase().contains("y") {
        sql_shell(&connection);
    }

    search.clear();

    let mut queries = [
        connection
            .prepare("SELECT id, ip_start, ip_end, country, stateprov, city FROM ips WHERE ip_start <= ?1 AND ip_end >= ?2 LIMIT 1")
            .unwrap(),
        connection
            .prepare("SELECT id, ip_start, ip_end, country, stateprov, city FROM ips NOT INDEXED WHERE ip_start <= ?1 AND ip_end >= ?2 LIMIT 1")
            .unwrap()
    ];

    println!("Run benchmarks?");
    stdin.read_line(&mut search).expect("Something went wrong!");
    if search.to_lowercase().contains("y") {
        benchmark(&connection, &mut queries);
    }

    loop {
        println!("enter ip between {} and {}", u32::MIN, u32::MAX);
        print!("search ip: ");
        search.clear();
        stdin.read_line(&mut search).expect("Something went wrong!");
        if search.contains("exit") {
            break;
        }

        let ip = string_to_ip(search.trim().to_string());

        println!(
            "\n\n... searching for ip: {ip} (dec) = {} (bin) ...",
            search.trim()
        );

        let now = Instant::now();

        let fetch = queries[if ip < 1216605195 { 0 } else { 1 }].query([ip, ip]);

        println!(
            "Fetched in:              {: >12}ns ~ {: >12}ms",
            now.elapsed().as_nanos(),
            now.elapsed().as_nanos() as f64 / 1_000_000.0
        );

        let data = fetch.unwrap().mapped(|row| {
            Ok(Record {
                id: row.get(0)?,
                ip_start: row.get(1)?,
                ip_end: row.get(2)?,
                country: row.get(3)?,
                stateprov: row.get(4)?,
                city: row.get(5)?,
            })
        });

        println!(
            "Fetched and parsed in:   {: >12}ns ~ {: >12}ms",
            now.elapsed().as_nanos(),
            now.elapsed().as_nanos() as f64 / 1_000_000.0
        );

        println!(
            "Found: {:?}",
            data.map(|b| {
                let a = b.unwrap();
                format!(
                    "{}, {}, {}",
                    a.country.clone(),
                    a.stateprov.clone(),
                    a.city.clone()
                )
            })
            .collect::<Vec<String>>()
        );

        println!("");
    }
}

fn benchmark(connection: &Connection, queries: &mut [Statement; 2]) {
    let mut fetch: Vec<Duration> = Vec::new();
    let mut parse: Vec<Duration> = Vec::new();

    (0..u32::MAX).step_by(32).for_each(|ip| {
        // let ip = rand::thread_rng().gen_range(0..u32::MAX);

        let start = Instant::now();

        let f = queries[if ip < 1216605195 { 0 } else { 1 }].query([ip, ip]);

        fetch.push(start.elapsed());

        let data = f.unwrap().mapped(|row| {
            Ok(Record {
                id: row.get(0)?,
                ip_start: row.get(1)?,
                ip_end: row.get(2)?,
                country: row.get(3)?,
                stateprov: row.get(4)?,
                city: row.get(5)?,
            })
        });

        parse.push(start.elapsed());
    });

    println!(
        "Average fetch: {}ns, worst fetch: {}ns, best fetch: {}ns",
        fetch.iter().map(|i| i.as_nanos()).sum::<u128>() as f64 / fetch.len() as f64,
        fetch.iter().max().unwrap().as_nanos(),
        fetch.iter().min().unwrap().as_nanos()
    );
    println!(
        "Average parse: {}ns, worst parse: {}ns, best parse: {}ns",
        parse.iter().map(|i| i.as_nanos()).sum::<u128>() as f64 / parse.len() as f64,
        parse.iter().max().unwrap().as_nanos(),
        parse.iter().min().unwrap().as_nanos()
    );
}

fn sql_shell(connection: &Connection) {
    let io = std::io::stdin();
    let mut input = String::new();

    println!("SQLite shell");
    loop {
        input.clear();
        io.read_line(&mut input).expect("Something went wrong!");
        if input.to_lowercase().contains("exit") {
            break;
        }
        let response = connection.query_row(input.as_str(), (), |r| {
            let s: String = r.get(0).unwrap();
            Ok(s)
        });
        println!("SQLite shell responded > {}", response.unwrap());
    }
}

fn init_data(connection: &Connection) {
    println!("Parsing data");
    // load data
    let data = data();

    connection
        .execute(
            "CREATE TABLE ips
            (id INTEGER, ip_start INTEGER, ip_end INTEGER,
            country TEXT, stateprov TEXT, city TEXT)",
            (),
        )
        .unwrap();

    println!("inserting");
    for (i, d) in data.iter().array_chunks::<512>().enumerate() {
        println!("{i}/{}", data.len() / 512);
        let t = format!(
            "INSERT INTO ips
                (id, ip_start, ip_end, country, stateprov, city)
                VALUES {}",
            d.map(|i| format!(
                "({}, {}, {}, '{}', '{}', '{}')",
                i.id, i.ip_start, i.ip_end, i.country, i.stateprov, i.city
            ))
            .join(",")
        );
        connection.execute(t.as_str(), ()).unwrap();
    }

    println!("Ready!");
}

struct Record {
    pub id: u32,
    pub ip_start: u32,
    pub ip_end: u32,
    pub country: String,
    pub stateprov: String,
    pub city: String,
}

fn data() -> Vec<Record> {
    let file = read_to_string(File::open("/home/pavel/Downloads/DbIp_Edu.sql").unwrap()).unwrap();

    let mut data = file.lines().skip(42).collect::<Vec<&str>>();

    data.reverse();

    let data = data.join("").replace("\n", "");

    Regex::new("\\),(\\(|\\n)")
        .unwrap()
        .split(&data)
        .map(|record| {
            &record[(record.find("VALUES (").map(|a| a as isize).unwrap_or(-8) + 8) as usize
                ..record.len()]
        })
        .filter_map(|record| {
            let values = Regex::new(",'")
                .unwrap()
                .split(&record.to_owned())
                .map(|it| it.to_string())
                .collect::<Vec<String>>();
            let mut values = values.iter();
            let mut spc = values.nth(0).unwrap().split(",");
            let id = spc.nth(0).unwrap();
            let ipv6 = spc.nth(0).unwrap();
            if ipv6 == "1" {
                return None;
            }
            Some(Record {
                id: u32::from_str_radix(id, 10).unwrap(),
                ip_start: string_to_ip(values.nth(0).unwrap().replace("'", "")),
                ip_end: string_to_ip(values.nth(0).unwrap().replace("'", "")),
                country: values.nth(0).unwrap().to_string().replace("'", ""),
                stateprov: values.nth(0).unwrap().to_string().replace("'", ""),
                city: values.nth(0).unwrap().to_string().replace("'", ""),
            })
        })
        .collect()
}

pub fn string_to_ip(ip: String) -> u32 {
    u32::from_str_radix(
        ip.split(".")
            .map(|e| format!("{:08b}", u8::from_str_radix(e, 10).unwrap()))
            .collect::<Vec<String>>()
            .join("")
            .as_str(),
        2,
    )
    .unwrap()
}
