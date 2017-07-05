dotnet restore
dotnet build -c release -f netcoreapp1.1 -r win10-x64
dotnet publish -c release -f netcoreapp1.1 -r win10-x64