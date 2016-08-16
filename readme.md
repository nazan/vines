# Vines

## What It Does

In short, this program keeps track of who is permitted to perform what actions on which resources. Of course, whatever resources, actors or actions in your particular problem domain must be input into Vines before it can do its magic. Resources are stored in Vines as a hierarchy.

## Run The Unit Tests

Clone this repo.

```
> git clone git@bitbucket.org:surfingcrab/vines.git
> cd vines
> composer install
```

Create database and generate the schema by importing the 'vines_schema.sql' file under 'config' directory.
Use a database administration tool like PhpMyAdmin to do this.

Once database created then make a copy of 'settings.example.ini' and rename it to 'settings.ini' in the same path. Change the parameters within 'settings.ini' so that it matches with the connection details to the database that was created in the previous step.

Make sure PHPUnit is setup properly in your development environment and run the following command. [Install PHPUnit](https://phpunit.de/manual/4.8/en/installation.html)

```
> phpunit -c tests/configuration.xml tests
```