db = db.getSiblingDB("owshq");

db.createUser(
    {
        user: "admin",
        pwd: "admin",
        roles: [
            {
                role: "readWrite",
                db: "owshq"
            }
        ]
    }
);