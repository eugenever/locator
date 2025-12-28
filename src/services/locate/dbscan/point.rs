use std::{
    fmt,
    hash::{Hash, Hasher},
};

use geo::{Distance, Haversine, Point as GeoPoint};

use super::{Algorithm, DBSCAN, Proximity};

/// Represents a 2 Dimensional point
#[derive(Clone, Copy, Debug)]
pub struct Point {
    pub id: u32,
    pub lon: f64,
    pub lat: f64,
}

// Must be implemented for the struct to be used in the algorithm
impl Proximity for Point {
    type Output = f64;

    fn distance(&self, other: &Point) -> f64 {
        let gp1 = GeoPoint::new(self.lon, self.lat);
        let gp2 = GeoPoint::new(other.lon, other.lat);
        Haversine::distance(gp1, gp2)
    }
}

// Need to implement our own hash function since you cannot derive the `Hash`
// trait for the `f64` primitive type
impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Point {
    fn eq(&self, other: &Point) -> bool {
        self.id == other.id
    }
}

impl Eq for Point {}

// Not required by the algorithm
impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(id: {}, lon: {}, lat: {})", self.id, self.lon, self.lat)
    }
}

#[cfg(test)]
mod tests {
    //! This is an example of implementing DBSCAN for a 2D cartesian system
    //!
    //! The struct `Point` is our main 'clusterable' type. The algorithm requires
    //! that a number of traits be implemented by this type before it can be used.
    //!
    //! In the tests function we define a number of 2D points and use the algorithm
    //! to cluster them accordingly and print out the clusters
    //!
    //!
    use super::{Algorithm, DBSCAN, Point};

    #[test]
    fn test_cluster_with_noise() {
        let point_tuples = vec![
            (37.902874, 55.711887),
            (37.899498, 55.714649),
            (37.898196, 55.714781),
            (37.898742, 55.713434),
        ];

        let points = point_tuples
            .into_iter()
            .enumerate()
            .map(|(id, pt)| Point {
                id: id as u32,
                lon: pt.0,
                lat: pt.1,
            })
            .collect::<Vec<_>>();

        let alg = DBSCAN::new(170.0, 0);

        // Clusters are returned as a `HashMap` whose keys are `Option<u32>` values
        // and whose values are a list of `Point` structs in this implementation.
        // The keys that have some value (`Some(value)`) represent clusters. Noise
        // elements have a key of `None`
        let cluster_results = alg.cluster(&points);
        for cluster in cluster_results.clusters() {
            print!("\nCluster: [");
            for cluster_point in cluster {
                print!(" {}", cluster_point)
            }
            println!(" ]");
        }

        print!("\nNoise: [");
        for noise_point in cluster_results.noise() {
            print!(" {}", noise_point);
        }
        println!(" ]");
    }

    #[test]
    fn test_cluster_with_noise2() {
        let point_tuples = vec![
            (38.915527, 44.916786),
            (37.899498, 55.714649),
            (37.898196, 55.714781),
        ];

        let points = point_tuples
            .into_iter()
            .enumerate()
            .map(|(id, pt)| Point {
                id: id as u32,
                lon: pt.0,
                lat: pt.1,
            })
            .collect::<Vec<_>>();

        let alg = DBSCAN::new(85.0, 0);
        let cluster_results = alg.cluster(&points);
        for cluster in cluster_results.clusters() {
            print!("\nCluster: [");
            for cluster_point in cluster {
                print!(" {}", cluster_point)
            }
            println!(" ]");
        }

        print!("\nNoise: [");
        for noise_point in cluster_results.noise() {
            print!(" {}", noise_point);
        }
        println!(" ]");
    }

    #[test]
    fn test_cluster_with_noise3() {
        let point_tuples = vec![
            (37.899498, 55.714649),
            (37.898196, 55.714781),
            (37.898742, 55.713434),
            (37.900895, 55.710936),
        ];

        let points = point_tuples
            .into_iter()
            .enumerate()
            .map(|(id, pt)| Point {
                id: id as u32,
                lon: pt.0,
                lat: pt.1,
            })
            .collect::<Vec<_>>();

        let alg = DBSCAN::new(150.0, 0);
        let cluster_results = alg.cluster(&points);
        for cluster in cluster_results.clusters() {
            print!("\nCluster: [");
            for cluster_point in cluster {
                print!(" {}", cluster_point)
            }
            println!(" ]");
        }

        print!("\nNoise: [");
        for noise_point in cluster_results.noise() {
            print!(" {}", noise_point);
        }
        println!(" ]");
    }

    #[test]
    fn test_cluster_success() {
        let point_tuples = vec![
            (37.899498, 55.714649),
            (37.898196, 55.714781),
            (37.898742, 55.713434),
        ];

        let points = point_tuples
            .into_iter()
            .enumerate()
            .map(|(id, pt)| Point {
                id: id as u32,
                lon: pt.0,
                lat: pt.1,
            })
            .collect::<Vec<_>>();

        let alg = DBSCAN::new(145.0, 0);
        let cluster_results = alg.cluster(&points);
        for cluster in cluster_results.clusters() {
            print!("\nCluster: [");
            for cluster_point in cluster {
                print!(" {}", cluster_point)
            }
            println!(" ]");
        }

        print!("\nNoise: [");
        for noise_point in cluster_results.noise() {
            print!(" {}", noise_point);
        }
        println!(" ]");
    }
}
